{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, ExtendedDefaultRules       #-}
{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase                        #-}
{-# LANGUAGE NoMonomorphismRestriction, OverloadedStrings, RecordWildCards #-}
{-# LANGUAGE ViewPatterns                                                  #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
module System.LXD ( LXDT, ContainerT, withContainer, Container
                  , LXDResult(..), AsyncClass(..), LXDResources(..)
                  , LXDError(..), Device(..)
                  , ContainerConfig(..), ContainerSource(..)
                  , LXDStatus(..), Interaction(..)
                  , LXDConfig, LXDServer(..)
                  , ExecOptions(..), ImageSpec(..), Alias, Fingerprint
                  , runLXDT, defaultExecOptions
                  , createContainer, cloneContainer
                  , execute, executeIn, listContainers
                  , writeFileBody, writeFileBodyIn
                  , writeFileStr, writeFileStrIn
                  , writeFileBS, writeFileBSIn
                  , writeFileLBS, writeFileLBSIn
                  , readFileOrListDirFrom
                  ) where
import           Control.Exception            (Exception)
import           Control.Lens                 ((%~), (&), (.~), _head)
import           Control.Monad.Catch          (MonadCatch, MonadThrow, throwM)
import           Control.Monad.Trans          (MonadIO (..), MonadTrans (..))
import           Control.Monad.Trans.Reader   (ReaderT (..), ask)
import           Data.Aeson                   (FromJSON (..), ToJSON (..))
import           Data.Aeson                   (Value, eitherDecode, encode)
import           Data.Aeson                   (genericToJSON, object)
import           Data.Aeson                   (withObject, withScientific, (.:))
import           Data.Aeson                   ((.=))
import qualified Data.Aeson                   as AE
import           Data.Aeson.Lens              (key)
import           Data.Aeson.Types             (camelTo2, defaultOptions)
import           Data.Aeson.Types             (fieldLabelModifier,
                                               omitNothingFields)
import qualified Data.Aeson.Types             as AE
import           Data.ByteString              (ByteString)
import qualified Data.ByteString.Char8        as BS
import qualified Data.ByteString.Lazy.Char8   as LBS
import qualified Data.Char                    as C
import           Data.Default                 (Default (..))
import           Data.HashMap.Lazy            (HashMap)
import qualified Data.HashMap.Lazy            as HM
import           Data.Maybe                   (fromJust, fromMaybe)
import           Data.Monoid                  ((<>))
import           Data.Scientific              (toBoundedInteger)
import           Data.Text                    (Text)
import qualified Data.Text                    as T
import qualified Data.Text.Encoding           as T
import           Data.Time                    (UTCTime, defaultTimeLocale,
                                               parseTimeM)
import           Data.Typeable                (Typeable)
import           GHC.Generics                 (Generic)
import           Network.HTTP.Client.Internal (Connection, Manager)
import           Network.HTTP.Client.Internal (defaultManagerSettings)
import           Network.HTTP.Client.Internal (makeConnection)
import           Network.HTTP.Client.Internal (managerRawConnection, newManager)
import           Network.HTTP.Conduit         (Request (..), RequestBody (..))
import           Network.HTTP.Conduit         (httpLbs, parseRequest)
import           Network.HTTP.Conduit         (responseBody, tlsManagerSettings)
import           Network.HTTP.Types.URI       (renderQuery)
import           Network.Socket               (Family (..), SockAddr (..))
import           Network.Socket               (SocketType (..), close, connect)
import           Network.Socket               (socket)
import qualified Network.Socket.ByteString    as BSSock

default (Text)

data LXDResult a = LXDSync { lxdStatus :: LXDStatus
                           , lxdMetadata :: a
                           }
                 | LXDAsync { lxdOperation     :: FilePath
                            , lxdStatus        :: LXDStatus
                            , lxdAsyncUUID :: ByteString
                            , lxdAsyncClass :: AsyncClass
                            , lxdAsyncCreated :: UTCTime
                            , lxdAsyncUpdated :: UTCTime
                            , lxdAsyncStatus  :: Text
                            , lxdAsyncStatusCode :: Int
                            , lxdAsyncResources :: LXDResources
                            , lxdAsyncMetadata  :: a
                            , lxdAsyncMayCancel :: Bool
                            , lxdAsyncError :: Text
                            }
                 | LXDError { lxdErrorCode     :: LXDStatus
                            , lxdErrorMessage  :: Text
                            , lxdErrorMetadata :: Value
                            }
                 deriving (Read, Show, Eq)

data LXDStatus = OperationCreated
               | Started
               | Stopped
               | Running
               | Cancelling
               | Pending
               | Starting
               | Stopping
               | Aborting
               | Freezing
               | Frozen
               | Thawed
               | Success
               | Failure
               | Cancelled
               | UnknownCode Int
               deriving (Read, Show, Eq, Ord)

statDic :: [(Int, LXDStatus)]
statDic = [(100,OperationCreated)
          ,(101,Started)
          ,(102,Stopped)
          ,(103,Running)
          ,(104,Cancelling)
          ,(105,Pending)
          ,(106,Starting)
          ,(107,Stopping)
          ,(108,Aborting)
          ,(109,Freezing)
          ,(110,Frozen)
          ,(111,Thawed)
          ,(200,Success)
          ,(400,Failure)
          ,(401,Cancelled)
          ]

instance Enum LXDStatus where
  toEnum i = fromMaybe (UnknownCode i) $ lookup i statDic
  fromEnum (UnknownCode i) = i
  fromEnum e = head [ i | (i, e') <- statDic, e == e']

instance FromJSON LXDStatus where
  parseJSON = withScientific "Status code" $ \s ->
    case toBoundedInteger s of
      Just i | 100 <= i && i <= 999 -> return $ toEnum i
             | otherwise -> fail "Status code must be between 100 and 999."
      Nothing -> fail "Status code must be integer"

instance FromJSON a => FromJSON (LXDResult a) where
  parseJSON = withObject "LXD Response" $ \obj -> do
    typ <- obj .: "type"
    case typ of
      "sync" -> LXDSync <$> obj .: "status_code"
                        <*> obj .: "metadata"
      "error" -> LXDError <$> obj .: "error_code"
                          <*> obj .: "error"
                          <*> obj .: "metadata"
      "async" -> do
        lxdStatus <- obj .: "status_code"
        lxdOperation <- obj .: "operation"
        AsyncMetaData{ asID = UUID lxdAsyncUUID
                     , asClass = lxdAsyncClass
                     , asCreatedAt = LXDTime lxdAsyncCreated
                     , asUpdatedAt = LXDTime lxdAsyncUpdated
                     , asMetadata = lxdAsyncMetadata
                     , asStatus   = lxdAsyncStatus
                     , asStatusCode = lxdAsyncStatusCode
                     , asResources = lxdAsyncResources
                     , asMayCancel = lxdAsyncMayCancel
                     , asErr = lxdAsyncError
                     } <- obj .: "metadata"
        return LXDAsync{..}
      _ -> fail ("Unknown result type: " ++ typ)

data AsyncClass = Task | WebSocket | Token
                deriving (Read, Show, Eq, Ord, Enum, Generic)

instance FromJSON AsyncClass where
  parseJSON = AE.genericParseJSON
              defaultOptions  { AE.sumEncoding = AE.UntaggedValue
                              , AE.constructorTagModifier = _head %~ C.toLower
                              }


data LXDResources = LXDResources { lxdContainers :: Maybe [Text]
                                 , lxdSnapshots  :: Maybe [Text]
                                 , lxdImages     :: Maybe [Text]
                                 }
                 deriving (Read, Show, Eq, Ord, Generic)

instance FromJSON LXDResources where
  parseJSON = AE.genericParseJSON
              defaultOptions { fieldLabelModifier = camelTo2 '-' . drop 3
                             , omitNothingFields  = True
                             }

newtype UUID = UUID ByteString
             deriving (Read, Show, Eq, Ord)

instance FromJSON UUID where
  parseJSON = AE.withText "MD5 hash" $ pure . UUID . T.encodeUtf8

newtype LXDTime = LXDTime UTCTime
                deriving (Read, Show, Eq, Ord)

instance FromJSON LXDTime where
  parseJSON = AE.withText "Date-time in YYYY-mm-ddTHH:MM:SS.qqqqqqq-00:00" $
    maybe (fail "illegal time format") (return . LXDTime) .
    parseTimeM False defaultTimeLocale "%FT%T%Q%z" . T.unpack

data AsyncMetaData a = AsyncMetaData { asID :: UUID
                                     , asClass :: AsyncClass
                                     , asCreatedAt :: LXDTime
                                     , asUpdatedAt :: LXDTime
                                     , asStatus  :: Text
                                     , asStatusCode :: Int
                                     , asResources :: LXDResources
                                     , asMetadata  :: a
                                     , asMayCancel :: Bool
                                     , asErr :: Text
                                     }
                   deriving (Read, Show, Eq, Ord, Generic)

instance FromJSON a => FromJSON (AsyncMetaData a) where
  parseJSON = AE.genericParseJSON
              defaultOptions { omitNothingFields = True
                             , fieldLabelModifier = camelTo2 '_' . drop 2
                             }

data LXDServer = Local
               | Remote { lxdServerHost :: String
                        , lxdServerPort :: Maybe Int
                        , lxdClientCert :: FilePath
                        , lxdClientKey  :: FilePath
                        , lxdPassword   :: ByteString
                        }
               deriving (Read, Show, Eq, Ord)

data LXDEnv = LXDEnv Manager String

newtype LXDT m a = LXDT (ReaderT LXDEnv m a)
                 deriving (Functor, Applicative, Monad,
                           MonadIO, MonadThrow, MonadCatch)

type Container = Text
newtype ContainerT m a = ContainerT (ReaderT Container (LXDT m) a)
                       deriving (Functor, Applicative,
                                 Monad, MonadIO,
                                 MonadThrow, MonadCatch
                                )

runLXDT :: (MonadIO m, MonadThrow m) => LXDServer -> LXDT m a -> m a
runLXDT Local (LXDT act) = do
  man <- liftIO newLocalManager
  runReaderT act $ LXDEnv  man "http://example.com"
runLXDT Remote{..} (LXDT act) = do
  man <- liftIO $ newManager tlsManagerSettings
  runReaderT act $ LXDEnv man $ "https://" ++ lxdServerHost ++ maybe "" ((':':).show) lxdServerPort

withContainer :: Container -> ContainerT m a -> LXDT m a
withContainer c (ContainerT act) = runReaderT act c

localSock :: IO Connection
localSock = do
  sock <- socket AF_UNIX Stream 0
  connect sock $ SockAddrUnix "/var/lib/lxd/unix.socket"
  makeConnection (BSSock.recv sock 8192) (BSSock.sendAll sock) (close sock)

newLocalManager :: IO Manager
newLocalManager =
  newManager
    defaultManagerSettings
    { managerRawConnection =
         return $ \ _ _ _ -> localSock
    }

type EndPoint = String

data LXDError = MalformedResponse { errorMessage :: String }
              | ServerError { errorCode :: LXDStatus, errorMessage :: String }
              deriving (Read, Show, Eq, Ord, Typeable)

instance Exception LXDError

request :: (FromJSON a, MonadThrow m, MonadIO m)
        => (Request -> Request) -> [Char] -> LXDT m (LXDResult a)
request modif ep = do
  LXDEnv man url <- LXDT ask
  rsp <- httpLbs (modif $ fromJust $ parseRequest $ url ++ "/1.0/" ++ ep) man
  either (throwM . MalformedResponse . (<> show (responseBody rsp))) return $
    eitherDecode $ responseBody rsp

get :: (FromJSON a, MonadThrow m, MonadIO m) => EndPoint -> LXDT m (LXDResult a)
get = request $ \a -> a { method = "GET" }

post :: (ToJSON a, MonadIO m, MonadThrow m, FromJSON b) => EndPoint -> a -> LXDT m (LXDResult b)
post ep bdy = request (\a -> a { method = "POST"
                               , requestBody = RequestBodyLBS $ encode bdy
                               })
                      ep

put :: (ToJSON a, MonadIO m, MonadThrow m, FromJSON b) => EndPoint -> a -> LXDT m (LXDResult b)
put ep bdy = request (\a -> a { method = "PUT"
                               , requestBody = RequestBodyLBS $ encode bdy
                               })
                      ep

delete :: (MonadIO m, MonadThrow m, FromJSON a) => [Char] -> LXDT m (LXDResult a)
delete = request $ \a -> a { method = "DELETE" }

temp :: String
temp = "https://192.168.56.2:8443"

listContainers :: (MonadIO m, MonadThrow m) => LXDT m [Container]
listContainers = fromSync =<< get "containers"

fromSync :: MonadThrow m => LXDResult a -> m a
fromSync LXDSync{..} = return lxdMetadata
fromSync LXDAsync{}  =
  throwM $ MalformedResponse "Asynchronous result returned instead of standard"
fromSync LXDError{..} =
  throwM $ ServerError lxdErrorCode $
  unlines [T.unpack lxdErrorMessage, LBS.unpack (encode lxdErrorMetadata)]

type LXDConfig = HashMap Text Text

type DevType = Text

data Device = Device { devPath :: FilePath
                     , devType :: DevType
                     }
            deriving (Read, Show, Eq, Ord, Generic)

instance ToJSON Device where
  toJSON = genericToJSON defaultOptions { fieldLabelModifier = camelTo2 '_' . drop 3
                                        , omitNothingFields = True
                                        }

type Alias = Text
type Fingerprint = ByteString

data ImageSpec = ImageAlias { imgAlias :: Text }
               | ImageFingerprint { imgFingerprint :: ByteString }
               | ImageProperties { imgOS :: Text
                                 , imgRelease :: Text
                                 , imgArchi   :: Text
                                 }
               deriving (Read, Show, Eq, Ord)

instance ToJSON ImageSpec where
  toJSON (ImageAlias txt) = object ["alias" .= txt]
  toJSON (ImageFingerprint p) = object ["fingerprint" .= T.decodeUtf8 p]
  toJSON (ImageProperties os rel arch) =
    object [ "properties" .=
             object ["os" .= os, "release" .= rel, "architecture" .= arch]
           ]

data ContainerSource = SourceImage
                       { csSourceImage :: ImageSpec }
                     | SourceCopy { csContainerOnly :: Bool
                                  , csSourceContainer :: Container
                                  }
                     deriving (Read, Show, Eq, Ord)

instance ToJSON ContainerSource where
  toJSON (SourceImage spec) = toJSON spec & key "type" .~ (toJSON "image")
  toJSON (SourceCopy only cont) =
    object [ "type" .= "copy"
           , "container_only" .= only
           , "source" .= cont
           ]

data ContainerConfig =
  ContainerConfig { cName :: Container
                  , cArchitecture :: Maybe Text
                  , cProfiles :: [Text]
                  , cEphemeral :: Bool
                  , cConfig    :: LXDConfig
                  , cDevices   :: HashMap Text Device
                  , cSource    :: ContainerSource
                  }
  deriving (Read, Show, Eq, Generic)

instance ToJSON ContainerConfig where
  toJSON = genericToJSON defaultOptions { fieldLabelModifier = camelTo2 '_' . drop 1
                                        , omitNothingFields = True
                                        }


createContainer :: (MonadThrow m, MonadIO m) => ContainerConfig -> LXDT m (LXDResult Value)
createContainer = post "containers"

cloneContainer :: (MonadIO m, MonadThrow m)
               => Container           -- ^ original container name
               -> Container           -- ^ new copied container name
               -> Bool                -- ^ is ephemeral?
               -> Bool                -- ^ copy without snapshot?
               -> LXDConfig           -- ^ misc configurations
               -> HashMap Text Device -- ^ device dictionary
               -> LXDT m (LXDResult Value)
cloneContainer csSourceContainer cName cEphemeral csContainerOnly cConfig cDevices =
  let cSource = SourceCopy{..}
      cArchitecture = Nothing
      cProfiles = []
  in  createContainer $ ContainerConfig {..}

data ExecOptions_ = ExecOptions_ { eeCommand :: [Text]
                                 , eeEnvironment :: HashMap Text Text
                                 , eeWaitForWebsocket :: Bool
                                 , eeRecordOutput :: Bool
                                 , eeInteractive :: Bool
                                 }
                  deriving (Read, Show, Eq, Generic)

instance ToJSON ExecOptions_ where
  toJSON = genericToJSON
           defaultOptions { fieldLabelModifier = camelTo2 '-' . drop 2
                          , omitNothingFields = True
                          }


data Interaction = NoInteraction
                 | RecordOnly
                 | Interactive
                 | Threeway
                 deriving (Read, Show, Eq, Ord)

data ExecOptions = ExecOptions { execInteraction :: Interaction
                               , execWorkingDir  :: Maybe FilePath
                               , execEnvironment :: HashMap Text Text
                               }
                 deriving (Read, Show, Eq)

instance Default ExecOptions where
  def = ExecOptions { execInteraction = NoInteraction
                    , execWorkingDir  = Nothing
                    , execEnvironment =
                      HM.fromList [("PATH", "/usr/bin:/usr/local/bin:/sbin:/usr/sbin:/bin")]
                    }

defaultExecOptions :: ExecOptions
defaultExecOptions = def

executeIn :: (MonadThrow m, MonadIO m)
          => Container -> Text -> [Text] -> ExecOptions
          -> LXDT m (LXDResult Value)
executeIn c cmd args ExecOptions{..} =
  let eeCommand = cmd : args
      eeEnvironment = maybe id (HM.insert "PWD" . T.pack) execWorkingDir execEnvironment
      (eeWaitForWebsocket, eeRecordOutput, eeInteractive) =
        case execInteraction of
          NoInteraction -> (False, False, False)
          RecordOnly    -> (False, True, False)
          Interactive   -> (True, False, True)
          Threeway      -> (True, False, False)
  in post ("containers/" <> T.unpack c <> "/exec") $
     ExecOptions_ {..}

execute :: (MonadIO m, MonadThrow m)
        => Text -> [Text] -> ExecOptions
        -> ContainerT m (LXDResult Value)
execute cmd args opts = ContainerT $ do
  c <- ask
  lift $ executeIn c cmd args opts

liftContainer3 :: Monad m
               => (Container -> t2 -> t1 -> t -> LXDT m a)
               -> t2 -> t1 -> t -> ContainerT m a
liftContainer3 f a b c = ContainerT $ do
  cnt <- ask
  lift $ f cnt a b c
{-# INLINE liftContainer3 #-}

liftContainer2 :: Monad m
               => (Container -> t1 -> t -> LXDT m a)
               -> t1 -> t -> ContainerT m a
liftContainer2 f a b = ContainerT $ do
  cnt <- ask
  lift $ f cnt a b
{-# INLINE liftContainer2 #-}

liftContainer :: Monad m
              => (Container -> t -> LXDT m a)
              -> t -> ContainerT m a
liftContainer f a = ContainerT $ do
  cnt <- ask
  lift $ f cnt a
{-# INLINE liftContainer #-}

fileEndPoint :: Container -> String -> String
fileEndPoint c fp =
   let q = renderQuery True [("path", Just $ BS.pack fp)]
   in "containers/" <> T.unpack c <> "/files" <> BS.unpack q

writeFileBodyIn :: (MonadIO m, MonadThrow m)
                => Text -> FilePath -> RequestBody -> LXDT m Value
writeFileBodyIn c fp body =
  fromSync =<< flip request (fileEndPoint c fp) (\r ->
  r { method = "POST"
    , requestBody = body
    })

writeFileBody :: (MonadThrow m, MonadIO m) => FilePath -> RequestBody -> ContainerT m Value
writeFileBody = liftContainer2 writeFileBodyIn

writeFileStrIn :: (MonadIO m, MonadThrow m)
            => Text -> FilePath -> String -> LXDT m Value
writeFileStrIn c fp = writeFileBodyIn c fp . RequestBodyLBS . LBS.pack

writeFileLBSIn :: (MonadIO m, MonadThrow m)
            => Text -> FilePath -> LBS.ByteString -> LXDT m Value
writeFileLBSIn c fp = writeFileBodyIn c fp . RequestBodyLBS

writeFileBSIn :: (MonadIO m, MonadThrow m)
              => Text -> FilePath -> BS.ByteString -> LXDT m Value
writeFileBSIn c fp = writeFileBodyIn c fp . RequestBodyBS

writeFileStr :: (MonadThrow m, MonadIO m) => FilePath -> String -> ContainerT m Value
writeFileStr = liftContainer2 writeFileStrIn

writeFileLBS :: (MonadThrow m, MonadIO m) => FilePath -> LBS.ByteString -> ContainerT m Value
writeFileLBS = liftContainer2 writeFileLBSIn

writeFileBS :: (MonadThrow m, MonadIO m) => FilePath -> BS.ByteString -> ContainerT m Value
writeFileBS = liftContainer2 writeFileBSIn

readFileOrListDirFrom :: (MonadThrow m, MonadIO m)
                      => Container -> FilePath -> LXDT m (Either [FilePath] String)
readFileOrListDirFrom c fp = do
  v <- fromSync =<< get (fileEndPoint c fp)
  case v of
    AE.Array{}  | AE.Success val <- AE.fromJSON v -> return $ Left val
    AE.String{} | AE.Success val <- AE.fromJSON v -> return $ Right val
    _ -> throwM $ MalformedResponse $
         "File list or string expected, but got: " <> LBS.unpack (encode v)

data Operation = Operation
               deriving (Read, Show, Eq, Ord)
