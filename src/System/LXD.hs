{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, ExtendedDefaultRules       #-}
{-# LANGUAGE FlexibleContexts, FlexibleInstances                           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, MultiParamTypeClasses #-}
{-# LANGUAGE NoMonomorphismRestriction, OverloadedStrings, RankNTypes      #-}
{-# LANGUAGE RecordWildCards, ScopedTypeVariables, TypeFamilies            #-}
{-# LANGUAGE UndecidableInstances, ViewPatterns                            #-}
{-# OPTIONS_GHC -fno-warn-name-shadowing #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
module System.LXD ( LXDT, ContainerT, withContainer, Container
                  , LXDResult(..), AsyncClass(..), LXDResources(..)
                  , LXDError(..), Device, defaultPermission
                  , ContainerConfig(..), ContainerSource(..)
                  , LXDStatus(..), Interaction(..), Permission (..)
                  , ContainerAction(..), setContainerState, setState
                  , LXDConfig, LXDServer(..), AsyncProcess
                  , ExecOptions(..), ImageSpec(..), Alias, Fingerprint
                  , runLXDT, defaultExecOptions, waitForProcessTimeout
                  , createContainer, cloneContainer, waitForProcess
                  , getProcessExitCode, cancelProcess
                  , execute, executeIn, listContainers
                  , readAsyncProcess, readAsyncOutput, closeStdin
                  , sourceAsyncOutput, sourceAsyncStdout, sourceAsyncStderr
                  , readAsyncStdout, readAsyncStderr, closeProcessIO
                  , writeFileBody, writeFileBodyIn, readAsyncProcessIn
                  , writeFileStr, writeFileStrIn, sinkAsyncProcess
                  , writeFileBS, writeFileBSIn, asyncStdinWriter
                  , writeFileLBS, writeFileLBSIn, runCommandIn, runCommand
                  , readFileOrListDirFrom, startContainer, start
                  , stopContainer, stop, killContainer, kill
                  ) where
import           Conduit                         (Consumer, Producer, Source,
                                                  concatC, mapM_C, repeatMC, sinkHandle,
                                                  repeatWhileMC, runResourceT,
                                                  sinkLazy, ($$), (.|))
import           Control.Applicative             ((<|>))
import           Control.Concurrent.Async.Lifted (concurrently, race_)
import           Control.Concurrent.Lifted       (fork, killThread)
import           Control.Concurrent.STM          (atomically, TMVar)
import System.IO (stderr,stdout)
import           Control.Concurrent.STM          (newEmptyTMVarIO, putTMVar,
                                                  readTMVar, tryReadTMVar)
import           Control.Concurrent.STM.TBMQueue (closeTBMQueue, newTBMQueueIO,
                                                  readTBMQueue, writeTBMQueue)
import           Control.Exception               (Exception)
import           Control.Exception.Lifted        (bracket, finally, handle,
                                                  throwIO)
import           Control.Lens                    ((%~), _head, (^?!))
import           Control.Monad                   (void, (<=<))
import Data.Aeson.Lens (key)
import           Control.Monad.Base              (MonadBase (..),
                                                  liftBaseDefault)
import           Control.Monad.Catch             (MonadThrow, MonadMask,onException,
                                                  MonadCatch, catch, throwM)
import           Control.Monad.Trans             (MonadIO (..), MonadTrans (..))
import           Control.Monad.Trans.Control     (MonadBaseControl (..),
                                                  MonadTransControl (..),
                                                  defaultLiftBaseWith,
                                                  defaultLiftWith,
                                                  defaultRestoreM,
                                                  defaultRestoreT)
import           Control.Monad.Trans.Reader      (ReaderT (..), ask)
import           Data.Aeson                      (FromJSON (..), ToJSON (..))
import           Data.Aeson                      (Value, eitherDecode, encode)
import           Data.Aeson                      (genericToJSON, object)
import           Data.Aeson                      (withObject, withScientific,
                                                  (.:))
import           Data.Aeson                      ((.=))
import qualified Data.Aeson                      as AE
import           Data.Aeson.Types                (camelTo2, defaultOptions)
import           Data.Aeson.Types                (fieldLabelModifier,
                                                  omitNothingFields)
import qualified Data.Aeson.Types                as AE
import           Data.ByteString                 (ByteString)
import qualified Data.ByteString.Char8           as BS
import qualified Data.ByteString.Lazy.Char8      as LBS
import qualified Data.Char                       as C
import           Data.Conduit.TMChan             (mergeSources)
import           Data.Conduit.TQueue             (sinkTBMQueue, sourceTBMQueue)
import           Data.Default                    (Default (..))
import           Data.HashMap.Lazy               (HashMap)
import qualified Data.HashMap.Lazy               as HM
import           Data.Maybe                      (fromJust, fromMaybe, isJust)
import           Data.Monoid                     ((<>))
import           Data.Scientific                 (toBoundedInteger)
import           Data.Text                       (Text)
import qualified Data.Text                       as T
import qualified Data.Text.Encoding              as T
import           Data.Time                       (UTCTime, defaultTimeLocale,
                                                  parseTimeM)
import           Data.Typeable                   (Typeable)
import           GHC.Generics                    (Generic)
import           Network.HTTP.Client.Internal    (Connection, Manager)
import           Network.HTTP.Client.Internal    (defaultManagerSettings)
import           Network.HTTP.Client.Internal    (makeConnection)
import           Network.HTTP.Client.Internal    (managerRawConnection,
                                                  newManager)
import           Network.HTTP.Conduit            (Request (..),
                                                  RequestBody (..))
import           Network.HTTP.Conduit            (httpLbs, parseRequest)
import           Network.HTTP.Conduit            (responseBody,
                                                  responseTimeoutNone,
                                                  tlsManagerSettings)
import           Network.HTTP.Types.URI          (renderQuery)
import           Network.Socket                  (Family (..), SockAddr (..))
import           Network.Socket                  (SocketType (..), close,
                                                  connect)
import           Network.Socket                  (PortNumber, Socket, socket)
import qualified Network.Socket.ByteString       as BSSock
import           Network.WebSockets              (ClientApp,
                                                  ConnectionException (..),
                                                  HandshakeException,
                                                  defaultConnectionOptions,
                                                  receiveData,
                                                  runClientWithSocket,
                                                  sendBinaryData, sendClose)
import           System.Exit                     (ExitCode (..))
import           System.Posix.Types              (FileMode, GroupID, UserID)
import           Wuss                            (runSecureClient)

default (Text)

concurrently_ :: MonadBaseControl IO f => f a -> f b -> f ()
concurrently_ a b = void $ concurrently  a b

data LXDResult a = LXDSync { lxdStatus :: LXDStatus
                           , lxdMetadata :: a
                           }
                 | LXDAsync { lxdAsyncOperation     :: String
                            , lxdStatus        :: LXDStatus
                            , lxdAsyncUUID     :: Text
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
        lxdAsyncOperation <- obj .: "operation"
        AsyncMetaData{ asID = lxdAsyncUUID
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

data AsyncClass = Task | Websocket | Token
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

data AsyncMetaData a = AsyncMetaData { asID :: Text
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
                        , lxdServerPort :: Maybe PortNumber
                        , lxdClientCert :: FilePath
                        , lxdClientKey  :: FilePath
                        , lxdPassword   :: ByteString
                        }
               deriving (Read, Show, Eq, Ord)

data LXDEnv = LXDEnv Manager LXDServer

newtype LXDT m a = LXDT { runLXDT_ :: ReaderT LXDEnv m a }
                 deriving (Functor, Applicative, Monad, MonadTrans,
                           MonadIO, MonadCatch, MonadThrow, MonadMask)

instance MonadBase n m => MonadBase n (LXDT m) where
  liftBase = liftBaseDefault

instance MonadBaseControl n m => MonadBaseControl n (LXDT m) where
  type StM (LXDT m) a = StM (ReaderT LXDEnv m) a
  liftBaseWith = defaultLiftBaseWith
  restoreM = defaultRestoreM

instance MonadTransControl LXDT where
  type StT LXDT a = StT (ReaderT LXDEnv) a
  liftWith = defaultLiftWith LXDT runLXDT_
  restoreT = defaultRestoreT LXDT

type Container = Text
newtype ContainerT m a = ContainerT (ReaderT Container (LXDT m) a)
                       deriving (Functor, Applicative,
                                 Monad, MonadIO,
                                 MonadCatch, MonadThrow, MonadMask
                                )

runWS :: MonadIO m => EndPoint -> ClientApp a -> LXDT m a
runWS ep app = LXDT $ ask >>= \case
  LXDEnv _ Local -> liftIO $ do
    sock <- localSock
    runClientWithSocket
      sock "[::]"
      ep
      defaultConnectionOptions
      []
      app
  LXDEnv _ Remote{..} -> liftIO $
    runSecureClient lxdServerHost (fromMaybe 8443 lxdServerPort) ep app


baseUrl :: Monad m => LXDT m [Char]
baseUrl = LXDT $ ask >>= \case
  LXDEnv _ Local -> return "http://localhost"
  LXDEnv _ Remote{..} ->
    return $ "https://" ++ lxdServerHost ++ maybe "" ((':':).show) lxdServerPort

runLXDT :: (MonadIO m, MonadCatch m) => LXDServer -> LXDT m a -> m a
runLXDT Local (LXDT act) = do
  man <- liftIO newLocalManager
  runReaderT act $ LXDEnv  man Local
runLXDT rem@Remote{..} (LXDT act) = do
  man <- liftIO $ newManager tlsManagerSettings
  runReaderT act $ LXDEnv man rem

withContainer :: Container -> ContainerT m a -> LXDT m a
withContainer c (ContainerT act) = runReaderT act c

localSock :: IO Socket
localSock = do
  sock <- socket AF_UNIX Stream 0
  connect sock $ SockAddrUnix "/var/lib/lxd/unix.socket"
  return sock

localConn :: IO Connection
localConn = do
  sock <- localSock
  makeConnection (BSSock.recv sock 8192) (BSSock.sendAll sock) (close sock)

newLocalManager :: IO Manager
newLocalManager =
  newManager
    defaultManagerSettings
    { managerRawConnection =
         return $ \ _ _ _ -> localConn
    }

type EndPoint = String

data LXDError = MalformedResponse { errorEndPoint :: EndPoint, errorMessage :: String }
              | ServerError { errorCode :: LXDStatus, errorMessage :: String }
              | WebSocketError { errorPos :: String, errorWebSock :: HandshakeException
                               }
              deriving (Show, Typeable)

instance Exception LXDError

request :: (FromJSON a, MonadCatch m, MonadIO m)
        => (Request -> Request) -> EndPoint -> LXDT m (LXDResult a)
request modif ep = do
  LXDEnv man _ <- LXDT ask
  url <- baseUrl
  rsp <- httpLbs (modif $ fromJust $ parseRequest $ url ++ ep) man
  either (throwM . MalformedResponse ep . (<> LBS.unpack (responseBody rsp))) return $
    eitherDecode $ responseBody rsp

get :: (FromJSON a, MonadCatch m, MonadIO m) => EndPoint -> LXDT m (LXDResult a)
get = request $ \a -> a { method = "GET" }

post :: (ToJSON a, MonadIO m, MonadCatch m, FromJSON b) => EndPoint -> a -> LXDT m (LXDResult b)
post ep bdy = request (\a -> a { method = "POST"
                               , requestBody = RequestBodyLBS $ encode bdy
                               })
                      ep

delete :: (MonadIO m, MonadCatch m, FromJSON a) => [Char] -> LXDT m (LXDResult a)
delete = request $ \a -> a { method = "DELETE" }

closeStdin :: MonadIO m => AsyncProcess -> LXDT m ()
closeStdin TaskProc{} = return ()
closeStdin ap = liftIO $ ahCloseStdin $ apHandle ap

listContainers :: (MonadIO m, MonadCatch m) => LXDT m [Container]
listContainers = fromSync =<< get "/1.0/containers"

fromSync :: MonadCatch m => LXDResult a -> m a
fromSync LXDSync{..} = return lxdMetadata
fromSync LXDAsync{}  =
  throwM $ MalformedResponse "" "Asynchronous result returned instead of standard"
fromSync LXDError{..} =
  throwM $ ServerError lxdErrorCode $
  unlines [T.unpack lxdErrorMessage, LBS.unpack (encode lxdErrorMetadata)]

fromAsync :: MonadCatch m => LXDResult a -> m (Operation, a)
fromAsync LXDAsync{..} = return (Operation lxdAsyncOperation, lxdAsyncMetadata)
fromAsync LXDSync{}  =
  throwM $ MalformedResponse "" "Synchronous result returned instead of asynchronous"
fromAsync LXDError{..} =
  throwM $ ServerError lxdErrorCode $
  unlines [T.unpack lxdErrorMessage, LBS.unpack (encode lxdErrorMetadata)]

data AsyncProcess = TaskProc { apOperation :: String }
                  | InteractiveProc { apOperation :: String
                                    , apISocket :: Text
                                    , apControl :: Text
                                    , apHandle :: AsyncHandle
                                    , apExitCode :: TMVar ExitCode
                                    }
                  | ThreewayProc { apOperation :: String
                                 , apStdin   :: Text
                                 , apStdout  :: Text
                                 , apStderr  :: Text
                                 , apControl :: Text
                                 , apHandle :: AsyncHandle
                                 , apExitCode :: TMVar ExitCode
                                 }

instance Show AsyncHandle where
  showsPrec _ SimpleHandle{..} = showString "<interactive handle>"
  showsPrec _ _ = showString "<threeway handle>"

newtype OpToAsync = OpToAsync (Operation -> IO AsyncProcess)

instance FromJSON OpToAsync where
  parseJSON AE.Null = return $ OpToAsync $ return . TaskProc . runOperation
  parseJSON a = flip (AE.withObject "fd-object") a $ \obj -> do
    AE.Object fd <- obj .: "fds"
    apControl <- fd .: "control"
    let apHandle = undefined
    let three = do
          apStdout <- fd .: "1"
          apStderr <- fd .: "2"
          apStdin  <- fd .: "0"
          return $ \ (Operation apOperation) -> do
            apExitCode <- liftIO newEmptyTMVarIO
            return ThreewayProc{..}
        inter = do
          apISocket <- fd .: "0"
          return $ \ (Operation apOperation) -> do
            apExitCode <- liftIO newEmptyTMVarIO
            return InteractiveProc{..}
    OpToAsync <$> (three <|> inter)

type LXDConfig = HashMap Text Text

type Device = HashMap Text Text

-- instance ToJSON Device where
--   toJSON = genericToJSON defaultOptions { fieldLabelModifier = camelTo2 '_' . drop 3
--                                         , omitNothingFields = True
--                                         }

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
                                  , csSource :: Container
                                  }
                     deriving (Read, Show, Eq, Ord, Generic)

instance ToJSON ContainerSource where
  toJSON (SourceImage spec) =
    case toJSON spec of
      ~(AE.Object dic) -> AE.Object $ HM.insert "type" (AE.String "image") dic
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


createContainer :: (MonadCatch m, MonadBaseControl IO m, MonadIO m)
                => ContainerConfig -> LXDT m ()
createContainer c = do
  ap <- fromAsync' $ post "/1.0/containers" c
  liftIO $ atomically $ readTMVar $ apExitCode ap
  return ()

cloneContainer :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
               => Container           -- ^ original container name
               -> Container           -- ^ new copied container name
               -> Bool                -- ^ is ephemeral?
               -> Bool                -- ^ copy without snapshot?
               -> LXDConfig           -- ^ misc configurations
               -> HashMap Text Device -- ^ device dictionary
               -> LXDT m ()
cloneContainer csSource cName cEphemeral csContainerOnly cConfig cDevices =
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
                               , execUID :: Maybe UserID
                               , execGID :: Maybe GroupID
                               }
                 deriving (Read, Show, Eq)

instance Default ExecOptions where
  def = ExecOptions { execInteraction = NoInteraction
                    , execWorkingDir  = Nothing
                    , execEnvironment =
                      HM.fromList [("PATH", "/usr/bin:/usr/local/bin:/sbin:/usr/sbin:/bin")]
                    , execGID = Nothing
                    , execUID = Nothing
                    }

defaultExecOptions :: ExecOptions
defaultExecOptions = def

waitForProcessTimeout :: (MonadIO m, MonadCatch m)
                      => Maybe Int -> AsyncProcess -> LXDT m (Maybe ExitCode)
waitForProcessTimeout mdur ap = do
  n <- (fmap intToExitCode . maybeAEResult . AE.fromJSON =<<)
    <$> waitForOperationTimeout mdur ap
  case n of
    Just{} -> return n
    Nothing -> getProcessExitCode ap

maybeAEResult :: AE.Result a -> Maybe a
maybeAEResult (AE.Success a) = Just a
maybeAEResult _ = Nothing

waitForOperationTimeout :: (MonadIO m, MonadCatch m)
                        => Maybe Int -> AsyncProcess -> LXDT m (Maybe Value)
waitForOperationTimeout mdur ap = do
  let q = maybe "" (BS.unpack . renderQuery True . pure . (,) "timeout" . Just . BS.pack . show) mdur
      l = fromSync =<< request (\r -> r { responseTimeout = responseTimeoutNone } )
                       (apOperation ap <> "/wait" <> q)
      r = fromSync =<< get (apOperation ap)
  l `catch` \(_ :: LXDError) -> r

instance FromJSON WrappedExitCode where
  parseJSON = withObject "Container dictionary" $ \obj ->
    WrappedExitCode . fmap intToExitCode <$> obj AE..:? "return"

newtype WrappedExitCode = WrappedExitCode (Maybe ExitCode)

discard :: Functor f => f Value -> f ()
discard = void

cancelProcess :: (MonadIO m, MonadCatch m) => AsyncProcess -> LXDT m ()
cancelProcess ap =
  discard $ fromSync =<< delete ("operations/" <> apOperation ap)

waitForProcess :: (MonadCatch m, MonadIO m) => AsyncProcess -> LXDT m (Maybe ExitCode)
waitForProcess = waitForProcessTimeout Nothing

getProcessExitCode :: (MonadIO m, MonadCatch m) => AsyncProcess -> LXDT m (Maybe ExitCode)
getProcessExitCode TaskProc{} = return Nothing
getProcessExitCode ap = liftIO $ atomically $ tryReadTMVar $ apExitCode ap

intToExitCode :: Int -> ExitCode
intToExitCode i = if i == 0 then ExitSuccess else ExitFailure i

executeIn :: (MonadCatch m, MonadIO m, MonadBaseControl IO m)
          => Container -> Text -> [Text] -> ExecOptions
          -> LXDT m AsyncProcess
executeIn c cmd args ExecOptions{..} = do
  let eeCommand =
        if isJust execGID || isJust execUID
        then "sudo" : foldMap (\a -> ["-g", "#" <> T.pack (show a)]) execGID
                   ++ foldMap (\a -> ["-u", "#" <> T.pack (show a)]) execUID
                   ++ ("--" : cmd : args)
        else cmd : args
      eeEnvironment = maybe id (HM.insert "PWD" . T.pack) execWorkingDir execEnvironment
      (eeWaitForWebsocket, eeRecordOutput, eeInteractive) =
        case execInteraction of
          NoInteraction -> (False, False, False)
          RecordOnly    -> (False, True, False)
          Interactive   -> (True, False, True)
          Threeway      -> (True, False, False)
  fromAsync' $ post ("/1.0/containers/" <> T.unpack c <> "/exec")  ExecOptions_ {..}

data AsyncHandle = SimpleHandle { ahStdin  :: ByteString -> IO ()
                                , ahOutput :: IO (Maybe ByteString)
                                , ahCloseStdin :: IO ()
                                , ahCloseProcess :: IO ()
                                }
                 | ThreeHandle { ahStdin  :: ByteString -> IO ()
                               , ahStdout :: IO (Maybe ByteString)
                               , ahStderr :: IO (Maybe ByteString)
                               , ahCloseStdin  :: IO ()
                               , ahCloseProcess :: IO ()
                               }

untilEndOf :: (MonadBaseControl IO m, MonadCatch m, MonadIO m)
           => LXDT m a -> AsyncProcess -> LXDT m ()
untilEndOf act ap = do
  flag <- liftIO newEmptyTMVarIO
  let timekeeper = do
        ext <- waitForProcess ap
        liftIO $ atomically (putTMVar flag ext)
  (act `concurrently_` timekeeper) `race_` liftIO (atomically $ readTMVar flag)

getAsyncHandle :: (MonadBaseControl IO m, MonadCatch m, MonadIO m)
               => AsyncProcess -> LXDT m (Maybe AsyncHandle)
getAsyncHandle TaskProc{} = return Nothing
getAsyncHandle ap@InteractiveProc{..} = Just <$> do
  let ep = wsEP ap apISocket
      cep = wsEP ap apControl
  (inCh, outCh) <- liftIO $ (,) <$> newTBMQueueIO 10 <*> newTBMQueueIO 10
  liftIO $ atomically $ writeTBMQueue inCh ""
  let close = atomically $ closeTBMQueue inCh >> closeTBMQueue outCh
      h ConnectionClosed = liftIO close
      h CloseRequest{} = liftIO close
      h e = throwIO e
      h' lab e = throwIO $ WebSocketError lab e
  let action = flip finally (liftIO close) $
               handle (h' "interactive") $ runWS ep $ \conn ->
               handle h $ flip finally (sendClose conn "") $ do
                 (repeatMC (receiveData conn) $$ sinkTBMQueue outCh True)
                   `concurrently_`
                   (sourceTBMQueue inCh $$ mapM_C (sendBinaryData conn))
      ctrl = runWS cep $ \conn -> sendClose conn ""
  tid <- fork $ (action `concurrently_` ctrl) `untilEndOf` ap
  let ahStdin  = atomically . writeTBMQueue inCh
      ahOutput = atomically $ readTBMQueue outCh
      ahCloseStdin    = atomically (writeTBMQueue inCh "" >> closeTBMQueue inCh)
      ahCloseProcess  = ahCloseStdin >> killThread tid >> close
  return $ SimpleHandle {..}
getAsyncHandle ap@ThreewayProc{..} = Just <$> do
  let iep = wsEP ap apStdin
      oep = wsEP ap apStdout
      eep = wsEP ap apStderr
      cep = wsEP ap apControl
  (inCh, outCh, errCh) <-
    liftIO $ (,,) <$> newTBMQueueIO 10
                  <*> newTBMQueueIO 10
                  <*> newTBMQueueIO 10
  liftIO $ atomically $ writeTBMQueue inCh ""
  let close = atomically $ closeTBMQueue inCh >> closeTBMQueue outCh >> closeTBMQueue errCh
      h ConnectionClosed = liftIO close
      h CloseRequest{} = liftIO close
      h e = throwM e
      h' lab e = throwM $ WebSocketError lab e
      iact = flip finally (liftIO $ atomically $ closeTBMQueue inCh) $
                handle (h' "stdin") $ runWS iep $ \conn ->
                handle h $ sourceTBMQueue inCh $$ mapM_C (sendBinaryData conn)
      oact = flip finally (liftIO $ atomically $ closeTBMQueue outCh) $
                handle (h' "stdout") $ runWS oep $ \conn ->
                handle h  $ repeatMC (receiveData conn) $$ sinkTBMQueue outCh True
      eact = flip finally (liftIO $ atomically $ closeTBMQueue errCh) $
                handle (h' "stderr") $ runWS eep $ \conn ->
                handle h  $ repeatMC (receiveData conn) $$ sinkTBMQueue errCh True
      cact = handle (h' "control") $ handle (h' "control") $ runWS cep $ \conn ->
                sendClose conn ""
  tid <- fork $ (iact `concurrently_` oact `concurrently_` eact `concurrently` cact) `untilEndOf` ap
  let ahStdin  = atomically . writeTBMQueue inCh
      ahStdout = atomically $ readTBMQueue outCh
      ahStderr = atomically $ readTBMQueue errCh
      ahCloseStdin = atomically (closeTBMQueue inCh)
      ahCloseProcess = ahCloseStdin >> killThread tid >> close
  return $ ThreeHandle {..}

wsEP :: AsyncProcess -> Text -> EndPoint
wsEP ap st =
  let q = BS.unpack $
          renderQuery True [("secret", Just $ T.encodeUtf8 st)]
  in apOperation ap <> "/websocket" <> q

execute :: (MonadIO m, MonadBaseControl IO m, MonadCatch m)
        => Text -> [Text] -> ExecOptions
        -> ContainerT m AsyncProcess
execute = liftContainer3 executeIn

-- | Same as execute, but maps stdout/stderr to local ones.
runCommandIn :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
             => Container -> Text -> [Text] -> ByteString -> ExecOptions -> LXDT m (Maybe ExitCode)
runCommandIn c cmd args input opts = do
  bracket (executeIn c cmd args opts { execInteraction = Threeway })
    closeProcessIO $ \ap -> do
    liftIO (fromMaybe (const $ return ()) (asyncStdinWriter ap) input)
      `finally` closeStdin ap
    snd <$> (sourceAsyncStdout ap $$ sinkHandle stdout)
              `concurrently` (sourceAsyncStderr ap $$ sinkHandle stderr)
              `concurrently` waitForProcess ap

runCommand :: (MonadBaseControl IO m, MonadCatch m, MonadIO m)
           => Text -> [Text] -> ByteString -> ExecOptions -> ContainerT m (Maybe ExitCode)
runCommand = liftContainer4 runCommandIn

asyncStdinWriter :: AsyncProcess -> Maybe (ByteString -> IO ())
asyncStdinWriter TaskProc{..} = Nothing
asyncStdinWriter ap = Just $ ahStdin $ apHandle ap

sinkAsyncProcess :: MonadIO m => AsyncProcess -> Consumer ByteString m ()
sinkAsyncProcess TaskProc{..} = mempty
sinkAsyncProcess ap =
  mapM_C (liftIO . ahStdin (apHandle ap))

sourcePopper :: MonadIO m => IO (Maybe ByteString) -> Producer m ByteString
sourcePopper prod = repeatWhileMC (liftIO prod) isJust .| concatC

sourceAsyncOutput :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
                  => AsyncProcess -> m (Source m ByteString)
sourceAsyncOutput TaskProc{..} = return $ return ()
sourceAsyncOutput InteractiveProc{..} = return $ sourcePopper $ ahOutput apHandle
sourceAsyncOutput p@ThreewayProc{} =
  runResourceT $ mergeSources [sourceAsyncStdout p, sourceAsyncStderr p] 20

sourceAsyncStdout :: MonadIO m => AsyncProcess -> Producer m ByteString
sourceAsyncStdout ThreewayProc{..} = sourcePopper $ ahStdout apHandle
sourceAsyncStdout _ = return ()

sourceAsyncStderr :: MonadIO m => AsyncProcess -> Producer m ByteString
sourceAsyncStderr ThreewayProc{..} = sourcePopper $ ahStderr apHandle
sourceAsyncStderr _ = return ()

liftContainer4 :: Monad m
               => (Container -> t3 -> t2 -> t1 -> t -> LXDT m a)
               -> t3 -> t2 -> t1 -> t -> ContainerT m a
liftContainer4 f d a b c = ContainerT $ do
  cnt <- ask
  lift $ f cnt d a b c
{-# INLINE liftContainer4 #-}

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
   in "/1.0/containers/" <> T.unpack c <> "/files" <> BS.unpack q


data Permission = Permission { fileUID :: Maybe UserID
                             , fileGID :: Maybe GroupID
                             , fileMode:: Maybe FileMode
                             }
                deriving (Read, Show, Eq, Ord)
defaultPermission :: Permission
defaultPermission = Permission Nothing Nothing Nothing

instance Default Permission where
  def = defaultPermission

writeFileBody :: (MonadCatch m, MonadIO m) => FilePath -> Permission -> RequestBody -> ContainerT m Value
writeFileBody = liftContainer3 writeFileBodyIn

writeFileBodyIn :: (MonadIO m, MonadCatch m)
                => Container -> FilePath -> Permission -> RequestBody -> LXDT m Value
writeFileBodyIn c fp Permission{..} body =
  let hds = [ ("X-LXD-uid", BS.pack $ show uid) | Just uid <- return fileUID ]
         ++ [ ("X-LXD-gid", BS.pack $ show uid) | Just uid <- return fileGID ]
         ++ [ ("X-LXD-mode", BS.pack $ show uid) | Just uid <- return fileMode ]
  in fromSync =<< flip request (fileEndPoint c fp)
     (\r ->
       r { method = "POST"
         , requestBody = body
         , requestHeaders = hds ++ requestHeaders r
         }
     )

writeFileStrIn :: (MonadIO m, MonadCatch m)
            => Container -> FilePath -> Permission -> String -> LXDT m Value
writeFileStrIn c fp perm = writeFileBodyIn c fp perm . RequestBodyLBS . LBS.pack

writeFileLBSIn :: (MonadIO m, MonadCatch m)
            => Container -> FilePath -> Permission -> LBS.ByteString -> LXDT m Value
writeFileLBSIn c perm fp = writeFileBodyIn c perm fp . RequestBodyLBS

writeFileBSIn :: (MonadIO m, MonadCatch m)
              => Container -> FilePath -> Permission -> BS.ByteString -> LXDT m Value
writeFileBSIn c perm fp = writeFileBodyIn c perm fp . RequestBodyBS

writeFileStr :: (MonadCatch m, MonadIO m) => FilePath -> Permission -> String -> ContainerT m Value
writeFileStr = liftContainer3 writeFileStrIn

writeFileLBS :: (MonadCatch m, MonadIO m) => FilePath -> Permission -> LBS.ByteString -> ContainerT m Value
writeFileLBS = liftContainer3 writeFileLBSIn

writeFileBS :: (MonadCatch m, MonadIO m) => FilePath -> Permission -> BS.ByteString -> ContainerT m Value
writeFileBS = liftContainer3 writeFileBSIn

readAsyncProcessIn :: (MonadBaseControl IO m, MonadIO m, MonadCatch m)
                   => Container -> Text -> [Text] -> ByteString
                   -> ExecOptions -> LXDT m (LBS.ByteString, LBS.ByteString)
readAsyncProcessIn c cmd args input opts = do
  bracket (executeIn c cmd args opts { execInteraction = Threeway })
          (liftIO . ahCloseProcess . apHandle) $ \ap -> do
    liftIO (fromJust (asyncStdinWriter ap) input) `finally` closeStdin ap
    (,) <$> (sourceAsyncStdout ap $$ sinkLazy)
        <*> (sourceAsyncStderr ap $$ sinkLazy)

readAsyncStdout :: AsyncProcess -> IO (Maybe ByteString)
readAsyncStdout ThreewayProc{..} = ahStdout apHandle
readAsyncStdout _ = return Nothing

readAsyncStderr :: AsyncProcess -> IO (Maybe ByteString)
readAsyncStderr ThreewayProc{..} = ahStderr apHandle
readAsyncStderr _ = return Nothing

readAsyncOutput :: AsyncProcess -> IO (Maybe ByteString)
readAsyncOutput InteractiveProc{..} = ahOutput apHandle
readAsyncOutput _ = return Nothing

readAsyncProcess :: (MonadCatch m, MonadIO m, MonadBaseControl IO m)
                 => Text -> [Text] -> ByteString -> ExecOptions
                 -> ContainerT m (LBS.ByteString, LBS.ByteString)
readAsyncProcess = liftContainer4 readAsyncProcessIn

readFileOrListDirFrom :: (MonadCatch m, MonadIO m)
                      => Container -> FilePath -> LXDT m (Either [FilePath] String)
readFileOrListDirFrom c fp = do
  v <- fromSync =<< get (fileEndPoint c fp)
  case v of
    AE.Array{}  | AE.Success val <- AE.fromJSON v -> return $ Left val
    AE.String{} | AE.Success val <- AE.fromJSON v -> return $ Right val
    _ -> throwM $ MalformedResponse (fileEndPoint c fp) $
         "File list or string expected, but got: " <> LBS.unpack (encode v)

closeProcessIO :: MonadIO m => AsyncProcess -> LXDT m ()
closeProcessIO TaskProc{} = return ()
closeProcessIO ap = liftIO $ ahCloseProcess $ apHandle ap

data ContainerAction = Stop     { actTimeout :: Int
                                , actStateful :: Bool
                                , actForce :: Bool
                                }
                     | Start    { actTimeout :: Int, actStateful :: Bool }
                     | Restart  { actTimeout :: Int, actForce :: Bool }
                     | Freeze   { actTimeout :: Int }
                     | Unfreeze { actTimeout :: Int }
                     deriving (Read, Show, Eq, Ord, Generic)

instance ToJSON ContainerAction where
  toJSON = genericToJSON
           defaultOptions
           { AE.sumEncoding = AE.TaggedObject "action" ""
           , AE.fieldLabelModifier = camelTo2 '_' . drop 3
           , AE.constructorTagModifier = _head %~ C.toLower
           }

fromAsync' :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
           => LXDT m (LXDResult OpToAsync) -> LXDT m AsyncProcess
fromAsync' act = do
  (op, OpToAsync p) <- fromAsync =<< act
  ap0 <- liftIO $ p op
  ti <- fork $
    case ap0 of
      TaskProc{} -> return ()
      _ -> flip onException (liftIO $ atomically $ putTMVar (apExitCode ap0) (ExitFailure (-1))) $ do
        ans <- asValue <$> (fromSync =<< request (\r -> r {responseTimeout = responseTimeoutNone }) (apOperation ap0 <> "/wait"))
        liftIO $ atomically $ putTMVar (apExitCode ap0) $
          intToExitCode $ fromJust $ maybeAEResult $ AE.fromJSON $ ans ^?! key "metadata" . key "return"
  mah <- getAsyncHandle ap0
  return $ maybe ap0 (\ah -> ap0 { apHandle = ah { ahCloseProcess = ahCloseProcess ah >> killThread ti} })
             mah

setContainerState :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
                  => Container -> ContainerAction -> LXDT m AsyncProcess
setContainerState c cs =
  fromAsync' $
  request (\q -> q { method = "PUT"
                   , requestBody = RequestBodyLBS $ encode cs })
    ("/1.0/containers/" <> T.unpack c <> "/state")

setState :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
         => ContainerAction -> ContainerT m AsyncProcess
setState = liftContainer setContainerState

startContainer :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
               => Container
               -> Int           -- ^ timeout
               -> Bool          -- ^ is stateful?
               -> LXDT m ()
startContainer c wait st = do
  ap <- setContainerState c (Start wait st)
  void $ fmap asValue <$> waitForOperationTimeout (Just wait) ap

start :: (MonadIO m, MonadCatch m, MonadBaseControl IO m) => Int -> Bool -> ContainerT m ()
start = liftContainer2 startContainer

stopContainer :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
              => Container
              -> Int           -- ^ timeout
              -> Bool          -- ^ is stateful?
              -> LXDT m Bool
stopContainer c wait st = do
  ap <- setContainerState c Stop { actTimeout  = wait
                                 , actForce    = False
                                 , actStateful = st
                                 }
  isJust . fmap asValue <$> waitForOperationTimeout (Just wait) ap

stop :: (MonadIO m, MonadCatch m, MonadBaseControl IO m) => Int -> Bool -> ContainerT m Bool
stop = liftContainer2 stopContainer

killContainer :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
              => Container
              -> Int           -- ^ timeout
              -> Bool          -- ^ is stateful?
              -> LXDT m Bool
killContainer c wait st = do
  ap <- setContainerState c Stop { actTimeout  = wait
                                 , actForce    = True
                                 , actStateful = st
                                 }
  isJust . fmap asValue <$> waitForOperationTimeout (Just wait) ap

kill :: (MonadIO m, MonadCatch m, MonadBaseControl IO m) => Int -> Bool -> ContainerT m Bool
kill = liftContainer2 killContainer

asValue :: Value -> Value
asValue = id

newtype Operation = Operation { runOperation :: String }
               deriving (Read, Show, Eq, Ord)
