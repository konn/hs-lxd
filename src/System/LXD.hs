{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, ExtendedDefaultRules       #-}
{-# LANGUAGE FlexibleContexts, FlexibleInstances                           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns, NoMonomorphismRestriction, OverloadedStrings  #-}
{-# LANGUAGE RankNTypes, RecordWildCards, ScopedTypeVariables              #-}
{-# LANGUAGE TypeFamilies, UndecidableInstances                            #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
{-# OPTIONS_GHC -Wredundant-constraints #-}
module System.LXD ( LXDT, ContainerT, withContainer, Container
                  , LXDResult(..), AsyncClass(..), LXDResources(..)
                  , LXDError(..), Device, defaultPermission
                  , ContainerConfig(..), ContainerSource(..)
                  , LXDStatus(..), Interaction(..), Permission (..)
                  , ContainerAction(..), setContainerState, setState
                  , LXDConfig, LXDServer(..), AsyncProcess
                  , ContainerInfo
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
                  , getContainerInfo , putContainerInfo
                  , stopContainer, stop, killContainer, kill, addCertificate
                  ) where
import           Conduit                         (Consumer, Producer, Source,
                                                  concatC, mapM_C, repeatMC,
                                                  repeatWhileMC, runResourceT,
                                                  sinkHandle, sinkLazy, ($$),
                                                  (.|))
import           Control.Applicative             ((<|>))
import           Control.Concurrent.Async.Lifted (Concurrently (..),
                                                  concurrently, race, race_)
import           Control.Concurrent.Lifted       (ThreadId, fork, killThread,
                                                  threadDelay)
import           Control.Concurrent.STM          (TMVar, atomically)
import           Control.Concurrent.STM          (newEmptyTMVarIO, putTMVar,
                                                  readTMVar, tryReadTMVar)
import           Control.Concurrent.STM.TBMQueue (closeTBMQueue, newTBMQueueIO,
                                                  readTBMQueue, writeTBMQueue)
import           Control.Exception               (Exception)
import           Control.Lens                    ((%~), (^?), _head)
import           Control.Monad                   (void)
import           Control.Monad.Base              (MonadBase (..),
                                                  liftBaseDefault)
import           Control.Monad.Catch             (MonadCatch, MonadMask,
                                                  MonadThrow, bracket, finally,
                                                  handle, onException, throwM)
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
                                                  (.:), (.:?), (.=))
import qualified Data.Aeson                      as AE
import           Data.Aeson.Lens                 (key)
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
import           Network.Connection              (ConnectionParams (..),
                                                  TLSSettings (..), connectTo,
                                                  connectionGetChunk,
                                                  connectionPut,
                                                  initConnectionContext)
import           Network.HTTP.Client.Internal    (Connection, Manager)
import           Network.HTTP.Client.Internal    (defaultManagerSettings)
import           Network.HTTP.Client.Internal    (makeConnection)
import           Network.HTTP.Client.Internal    (managerRawConnection,
                                                  newManager)
import           Network.HTTP.Conduit            (Request (..),
                                                  RequestBody (..), httpLbs,
                                                  mkManagerSettings,
                                                  parseRequest, responseBody,
                                                  responseTimeoutNone)
import           Network.HTTP.Types.URI          (renderQuery)
import           Network.Socket                  (Family (..), SockAddr (..))
import           Network.Socket                  (SocketType (..), close,
                                                  connect)
import           Network.Socket                  (PortNumber, Socket, socket)
import qualified Network.Socket.ByteString       as BSSock
import           Network.TLS                     (ClientParams (..),
                                                  credentialLoadX509,
                                                  defaultParamsClient,
                                                  onCertificateRequest,
                                                  onServerCertificate,
                                                  supportedCiphers)
import           Network.TLS.Extra.Cipher        (ciphersuite_all)
import           Network.WebSockets              (ClientApp,
                                                  ConnectionException (..),
                                                  HandshakeException,
                                                  defaultConnectionOptions,
                                                  receiveData,
                                                  runClientWithSocket,
                                                  runClientWithStream,
                                                  sendBinaryData, sendClose)
import           Network.WebSockets.Stream       (makeStream)
import           System.Exit                     (ExitCode (..))
import           System.IO                       (stderr, stdout)
import           System.Posix.Types              (FileMode, GroupID, UserID)

default (Text, Int)

concurrently_ :: MonadBaseControl IO f => f a -> f b -> f ()
concurrently_ a b = void $ concurrently  a b

type ContainerInfo = Value
data LXDResult a = LXDSync { lxdStatus   :: LXDStatus
                           , lxdMetadata :: a
                           }
                 | LXDAsync { lxdAsyncOperation  :: String
                            , lxdStatus          :: LXDStatus
                            , lxdAsyncUUID       :: Text
                            , lxdAsyncClass      :: AsyncClass
                            , lxdAsyncCreated    :: UTCTime
                            , lxdAsyncUpdated    :: UTCTime
                            , lxdAsyncStatus     :: Text
                            , lxdAsyncStatusCode :: Int
                            , lxdAsyncResources  :: LXDResources
                            , lxdAsyncMetadata   :: a
                            , lxdAsyncMayCancel  :: Bool
                            , lxdAsyncError      :: Text
                            , lxdAsyncExitCode   :: TMVar ExitCode
                            , lxdAsyncWaiter     :: ThreadId
                            }
                 | LXDError { lxdErrorCode     :: LXDStatus
                            , lxdErrorMessage  :: Text
                            , lxdErrorMetadata :: Maybe Value
                            }
                 deriving (Eq)

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
  fromEnum e               = head [ i | (i, e') <- statDic, e == e']

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
                          <*> obj .:? "metadata"
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
        let lxdAsyncExitCode = undefined
            lxdAsyncWaiter = undefined
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

data AsyncMetaData a = AsyncMetaData { asID         :: Text
                                     , asClass      :: AsyncClass
                                     , asCreatedAt  :: LXDTime
                                     , asUpdatedAt  :: LXDTime
                                     , asStatus     :: Text
                                     , asStatusCode :: Int
                                     , asResources  :: LXDResources
                                     , asMetadata   :: a
                                     , asMayCancel  :: Bool
                                     , asErr        :: Text
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
                        , lxdPassword   :: Maybe ByteString
                        }
               deriving (Read, Show, Eq, Ord)

data LXDEnv = LXDLocalEnv Manager
            | LXDRemoteEnv Manager String (Maybe PortNumber) TLSSettings

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

runWS :: (MonadBaseControl IO m) => EndPoint -> ClientApp a -> LXDT m a
runWS ep app = LXDT $ ask >>= \case
  LXDLocalEnv{} -> liftBase $ do
    sock <- localSock
    runClientWithSocket
      sock "[::]"
      ep
      defaultConnectionOptions
      []
      app
  LXDRemoteEnv _ host mport tlsSettings -> liftBase $ do
    let port = fromMaybe 8443 mport
        conParams = ConnectionParams { connectionHostname = host
                                     , connectionPort = port
                                     , connectionUseSecure = Just tlsSettings
                                     , connectionUseSocks = Nothing
                                     }
    ctx  <- initConnectionContext
    conn <- connectTo ctx conParams
    stream <- makeStream
              (Just <$> connectionGetChunk conn)
              (maybe (return ()) (connectionPut conn . LBS.toStrict))
    liftBase $ runClientWithStream
      stream
      host
      ep
      defaultConnectionOptions
      []
      app


baseUrl :: Monad m => LXDT m [Char]
baseUrl = LXDT $ ask >>= \case
  LXDLocalEnv{} -> return "http://localhost"
  LXDRemoteEnv _ host mport _ ->
    return $ "https://" ++ host ++ maybe ":8443" ((':':).show) mport

runLXDT :: (MonadBaseControl IO m, MonadThrow m) => LXDServer -> LXDT m a -> m a
runLXDT Local (LXDT act) = do
  man <- liftBase newLocalManager
  runReaderT act $ LXDLocalEnv man
runLXDT Remote{..} (LXDT act) = do
  creds <- either (throwM . TLSError) return
           =<< liftBase (credentialLoadX509 lxdClientCert lxdClientKey)
  let hooks = def { onCertificateRequest = const $ return $ Just creds
                  , onServerCertificate = \_ _ _ _ -> return []
                  }
      clParams = (defaultParamsClient lxdServerHost "")
                 { clientHooks = hooks
                 , clientSupported = def { supportedCiphers = ciphersuite_all
                                         }
                 }
      tlsConf = TLSSettings clParams
      manSettings = mkManagerSettings tlsConf Nothing
  man <- liftBase $ newManager manSettings
  runReaderT act $ LXDRemoteEnv man lxdServerHost lxdServerPort tlsConf

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
              | TLSError { errorMessage :: String }
              deriving (Show, Typeable)

instance Exception LXDError

askManager :: Monad m => LXDT m Manager
askManager = LXDT $ ask >>= \case
  LXDLocalEnv man -> return man
  LXDRemoteEnv man _ _ _ -> return man

request :: (FromJSON a, MonadCatch m, MonadBaseControl IO m, MonadIO m)
        => (Request -> Request) -> EndPoint -> LXDT m (LXDResult a)
request modif ep = do
  man <- askManager
  url <- baseUrl
  rsp <- httpLbs (modif $ fromJust $ parseRequest $ url ++ ep) man
  r <- either (throwM . MalformedResponse ep . (<> LBS.unpack (responseBody rsp))) return $
    eitherDecode $ responseBody rsp
  case r of
    as@LXDAsync{lxdAsyncOperation} -> do
      lxdAsyncExitCode <- liftBase newEmptyTMVarIO
      let unkExc = liftBase $ atomically $ putTMVar lxdAsyncExitCode (ExitFailure (-1))
      lxdAsyncWaiter <-
        fork $ flip onException unkExc $ do
          let ep' =  lxdAsyncOperation <> "/wait"
          ans <- fromSync =<< request (\q -> q {responseTimeout = responseTimeoutNone }) ep'
          let ec = intToExitCode $ fromMaybe (- 1) $
                   maybeAEResult . AE.fromJSON =<< asValue ans ^? key "metadata" . key "return"
          liftBase $ atomically $ putTMVar lxdAsyncExitCode ec
      return as{lxdAsyncExitCode, lxdAsyncWaiter}
    _ -> return r

get :: (FromJSON a, MonadCatch m, MonadBaseControl IO m, MonadIO m) => EndPoint -> LXDT m (LXDResult a)
get = request $ \a -> a { method = "GET" }

post :: (ToJSON a, MonadBaseControl IO m, MonadIO m, MonadCatch m, FromJSON b)
     => EndPoint -> a -> LXDT m (LXDResult b)
post ep bdy = request (\a -> a { method = "POST"
                               , requestBody = RequestBodyLBS $ encode bdy
                               })
                      ep

delete :: (MonadBaseControl IO m, MonadIO m, MonadCatch m, FromJSON a) => [Char] -> LXDT m (LXDResult a)
delete = request $ \a -> a { method = "DELETE" }

closeStdin :: (MonadBaseControl IO m) => AsyncProcess -> LXDT m ()
closeStdin TaskProc{} = return ()
closeStdin ap         = liftBase $ ahCloseStdin $ apHandle ap

listContainers :: (MonadBaseControl IO m, MonadIO m, MonadCatch m) => LXDT m [Container]
listContainers = fromSync =<< get "/1.0/containers"

fromSync :: MonadCatch m => LXDResult a -> m a
fromSync LXDSync{..} = return lxdMetadata
fromSync LXDAsync{}  =
  throwM $ MalformedResponse "" "Asynchronous result returned instead of standard"
fromSync LXDError{..} =
  throwM $ ServerError lxdErrorCode $
  unlines [T.unpack lxdErrorMessage, LBS.unpack (encode lxdErrorMetadata)]

data AsyncProcess = TaskProc { apOperation :: String, apExitCode :: TMVar ExitCode }
                  | InteractiveProc { apOperation :: String
                                    , apISocket   :: Text
                                    , apControl   :: Text
                                    , apHandle    :: AsyncHandle
                                    , apExitCode  :: TMVar ExitCode
                                    }
                  | ThreewayProc { apOperation :: String
                                 , apStdin     :: Text
                                 , apStdout    :: Text
                                 , apStderr    :: Text
                                 , apControl   :: Text
                                 , apHandle    :: AsyncHandle
                                 , apExitCode  :: TMVar ExitCode
                                 }

instance Show AsyncProcess where
  showsPrec d TaskProc{..} = showParen (d > 10) $ showString "TaskProc " . shows apOperation
  showsPrec _ InteractiveProc{..} =
    showString "InteractiveProc { apOperation = " . shows apOperation
                  . showString ", apISocket = " . shows apISocket
                  . showString ", apControl = " . shows apControl
                  . showString ", apHandle = " . showsPrec 10 apHandle
                  . showChar '}'
  showsPrec _ ThreewayProc{..} =
    showString "InteractiveProc { apOperation = " . shows apOperation
                  . showString ", apStdin = " . shows apStdin
                  . showString ", apStdout = " . shows apStdout
                  . showString ", apStderr = " . shows apStderr
                  . showString ", apControl = " . shows apControl
                  . showString ", apHandle = " . showsPrec 10 apHandle
                  . showChar '}'

instance Show AsyncHandle where
  showsPrec _ SimpleHandle{..} = showString "<interactive handle>"
  showsPrec _ _                = showString "<threeway handle>"

newtype OpToAsync = OpToAsync (Operation -> TMVar ExitCode -> IO AsyncProcess)

instance FromJSON OpToAsync where
  parseJSON AE.Null = return $ OpToAsync $ (return .) . TaskProc . runOperation
  parseJSON a = flip (AE.withObject "fd-object") a $ \obj -> do
    AE.Object fd <- obj .: "fds"
    apControl <- fd .: "control"
    let apHandle = undefined
    let three = do
          apStdout <- fd .: "1"
          apStderr <- fd .: "2"
          apStdin  <- fd .: "0"
          return $ \ (Operation apOperation) apExitCode -> do
            return ThreewayProc{..}
        inter = do
          apISocket <- fd .: "0"
          return $ \ (Operation apOperation) apExitCode -> do
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
               | ImageProperties { imgOS      :: Text
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
                                  , csSource        :: Container
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
  ContainerConfig { cName         :: Container
                  , cArchitecture :: Maybe Text
                  , cProfiles     :: [Text]
                  , cEphemeral    :: Bool
                  , cConfig       :: LXDConfig
                  , cDevices      :: HashMap Text Device
                  , cSource       :: ContainerSource
                  }
  deriving (Read, Show, Eq, Generic)

instance ToJSON ContainerConfig where
  toJSON = genericToJSON defaultOptions { fieldLabelModifier = camelTo2 '_' . drop 1
                                        , omitNothingFields = True
                                        }


createContainer :: (MonadMask m, MonadBaseControl IO m, MonadIO m)
                => ContainerConfig -> LXDT m ()
createContainer c = do
  ap <- fromAsync $ post "/1.0/containers" c
  void $ waitForOperationTimeout Nothing ap

cloneContainer :: (MonadBaseControl IO m, MonadIO m, MonadMask m)
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

data ExecOptions_ = ExecOptions_ { eeCommand          :: [Text]
                                 , eeEnvironment      :: HashMap Text Text
                                 , eeWaitForWebsocket :: Bool
                                 , eeRecordOutput     :: Bool
                                 , eeInteractive      :: Bool
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
                               , execUID         :: Maybe UserID
                               , execGID         :: Maybe GroupID
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

waitForProcessTimeout :: (MonadBaseControl IO m, MonadIO m)
                      => Maybe Int -> AsyncProcess -> LXDT m (Maybe ExitCode)
waitForProcessTimeout mdur ap = waitForOperationTimeout mdur ap

maybeAEResult :: AE.Result a -> Maybe a
maybeAEResult (AE.Success a) = Just a
maybeAEResult _              = Nothing

waitForOperationTimeout :: (MonadBaseControl IO m, MonadIO m)
                        => Maybe Int -> AsyncProcess -> LXDT m (Maybe ExitCode)
waitForOperationTimeout mdur ap =
  let await = liftIO $ atomically (readTMVar $ apExitCode ap)
      procTask i =
        case ap of
          TaskProc{} | i == ExitFailure (-1) -> ExitSuccess
          _          -> i
  in case mdur of
    Nothing -> Just . procTask <$> await
    Just d  -> either (Just . procTask) (const Nothing) <$> await `race` liftIO (threadDelay $ d * 10^6)

instance FromJSON WrappedExitCode where
  parseJSON = withObject "Container dictionary" $ \obj ->
    WrappedExitCode . fmap intToExitCode <$> obj AE..:? "return"

newtype WrappedExitCode = WrappedExitCode (Maybe ExitCode)

discard :: Functor f => f Value -> f ()
discard = void

cancelProcess :: (MonadBaseControl IO m, MonadIO m, MonadCatch m) => AsyncProcess -> LXDT m ()
cancelProcess ap =
  discard $ fromSync =<< delete ("operations/" <> apOperation ap)

waitForProcess :: (MonadBaseControl IO m, MonadIO m) => AsyncProcess -> LXDT m ExitCode
waitForProcess = fmap fromJust . waitForProcessTimeout Nothing

getProcessExitCode :: (MonadBaseControl IO m) => AsyncProcess -> LXDT m (Maybe ExitCode)
getProcessExitCode TaskProc{} = return Nothing
getProcessExitCode ap = liftBase $ atomically $ tryReadTMVar $ apExitCode ap

intToExitCode :: Int -> ExitCode
intToExitCode i = if i == 0 then ExitSuccess else ExitFailure i

executeIn :: (MonadMask m, MonadBaseControl IO m, MonadIO m)
          => Container -> Text -> [Text] -> ExecOptions
          -> LXDT m AsyncProcess
executeIn c cmd args ExecOptions{..} = do
  let eeCommand =
        if isJust execGID || isJust execUID
        then "sudo" : foldMap (\a -> ["-g", "#" <> T.pack (show a)]) execGID
                   ++ foldMap (\a -> ["-u", "#" <> T.pack (show a)]) execUID
                   ++ HM.foldrWithKey (\k v a -> (k <> "=" <> v) : a)
                      (foldMap (\t -> ["PWD="<>T.pack t]) execWorkingDir)
                       execEnvironment
                   ++ ("--" : cmd : args)
        else cmd : args
      eeEnvironment = maybe id (HM.insert "PWD" . T.pack) execWorkingDir execEnvironment
      (eeWaitForWebsocket, eeRecordOutput, eeInteractive) =
        case execInteraction of
          NoInteraction -> (False, False, False)
          RecordOnly    -> (False, True, False)
          Interactive   -> (True, False, True)
          Threeway      -> (True, False, False)
  liftIO $ putStrLn $ "exec: " <> show eeCommand
  fromAsync $ post ("/1.0/containers/" <> T.unpack c <> "/exec")  ExecOptions_ {..}

data AsyncHandle = SimpleHandle { ahStdin        :: ByteString -> IO ()
                                , ahOutput       :: IO (Maybe ByteString)
                                , ahCloseStdin   :: IO ()
                                , ahCloseProcess :: IO ()
                                }
                 | ThreeHandle { ahStdin        :: ByteString -> IO ()
                               , ahStdout       :: IO (Maybe ByteString)
                               , ahStderr       :: IO (Maybe ByteString)
                               , ahCloseStdin   :: IO ()
                               , ahCloseProcess :: IO ()
                               }

untilEndOf :: (MonadBaseControl IO m, MonadIO m)
           => LXDT m a -> AsyncProcess -> LXDT m ()
untilEndOf act ap = act `race_` waitForProcess ap

getAsyncHandle :: (MonadBaseControl IO m, MonadIO m, MonadMask m)
               => AsyncProcess -> LXDT m (Maybe AsyncHandle)
getAsyncHandle TaskProc{} = return Nothing
getAsyncHandle ap@InteractiveProc{..} = Just <$> do
  let ep = wsEP ap apISocket
      cep = wsEP ap apControl
  (inCh, outCh) <- liftBase $ (,) <$> newTBMQueueIO 10 <*> newTBMQueueIO 10
  liftBase $ atomically $ writeTBMQueue inCh ""
  let cl = atomically $ closeTBMQueue inCh >> closeTBMQueue outCh
      h ConnectionClosed = liftBase cl
      h CloseRequest{}   = liftBase cl
      h e                = throwM e
      h' lab e = throwM $ WebSocketError lab e
  let action = flip finally (liftBase cl) $
               handle (h' "interactive") $ runWS ep $ \conn ->
               handle h $ flip finally (sendClose conn "") $ do
                 (repeatMC (receiveData conn) $$ sinkTBMQueue outCh True)
                   `concurrently_`
                   (sourceTBMQueue inCh $$ mapM_C (sendBinaryData conn))
      ctrl = runWS cep $ \conn -> sendClose conn ""
  tid <- fork $ (action `concurrently_` ctrl) -- `untilEndOf` ap
  let ahStdin  = atomically . writeTBMQueue inCh
      ahOutput = atomically $ readTBMQueue outCh
      ahCloseStdin    = atomically (writeTBMQueue inCh "" >> closeTBMQueue inCh)
      ahCloseProcess  = ahCloseStdin >> killThread tid >> cl
  return $ SimpleHandle {..}
getAsyncHandle ap@ThreewayProc{..} = Just <$> do
  let iep = wsEP ap apStdin
      oep = wsEP ap apStdout
      eep = wsEP ap apStderr
      cep = wsEP ap apControl
  (inCh, outCh, errCh) <-
    liftBase $ (,,) <$> newTBMQueueIO 10
                  <*> newTBMQueueIO 10
                  <*> newTBMQueueIO 10
  liftBase $ atomically $ writeTBMQueue inCh ""
  let finish = atomically $ closeTBMQueue inCh >> closeTBMQueue outCh >> closeTBMQueue errCh
      h ConnectionClosed = liftBase finish
      h CloseRequest{}   = liftBase finish
      h e                = throwM e
      h' lab e = throwM $ WebSocketError lab e
      iact = flip finally (liftBase $ atomically $ closeTBMQueue inCh) $
                handle (h' "stdin") $ runWS iep $ \conn ->
                handle h $ sourceTBMQueue inCh $$ mapM_C (sendBinaryData conn)
      oact = flip finally (liftBase $ atomically $ closeTBMQueue outCh) $
                handle (h' "stdout") $ runWS oep $ \conn ->
                handle h  $ repeatMC (receiveData conn) $$ sinkTBMQueue outCh True
      eact = flip finally (liftBase $ atomically $ closeTBMQueue errCh) $
                handle (h' "stderr") $ runWS eep $ \conn ->
                handle h  $ repeatMC (receiveData conn) $$ sinkTBMQueue errCh True
      cact = handle (h' "control") $ handle (h' "control") $ runWS cep $ \conn ->
                sendClose conn ""
  tid <- fork $ (iact `concurrently_` oact `concurrently_` eact `concurrently` cact) `untilEndOf` ap
  let ahStdin  = atomically . writeTBMQueue inCh
      ahStdout = atomically $ readTBMQueue outCh
      ahStderr = atomically $ readTBMQueue errCh
      ahCloseStdin = atomically (closeTBMQueue inCh)
      ahCloseProcess = ahCloseStdin >> killThread tid >> finish
  return $ ThreeHandle {..}

expectNull :: MonadThrow m => EndPoint -> Value -> m ()
expectNull _  AE.Null = return ()
expectNull ep b       =
  throwM $ MalformedResponse ep $ "Expected null, but got: " <> LBS.unpack (encode b)

wsEP :: AsyncProcess -> Text -> EndPoint
wsEP ap st =
  let q = BS.unpack $
          renderQuery True [("secret", Just $ T.encodeUtf8 st)]
  in apOperation ap <> "/websocket" <> q

execute :: (MonadBaseControl IO m, MonadIO m, MonadMask m)
        => Text -> [Text] -> ExecOptions
        -> ContainerT m AsyncProcess
execute = liftContainer3 executeIn

-- | Same as execute, but maps stdout/stderr to local ones.
runCommandIn :: (MonadMask m, MonadBaseControl IO m, MonadIO m)
             => Container -> Text -> [Text] -> ByteString -> ExecOptions -> LXDT m ExitCode
runCommandIn c cmd args input opts = do
  bracket (executeIn c cmd args opts { execInteraction = Threeway })
    closeProcessIO $ \ap -> do
    liftBase (fromMaybe (const $ return ()) (asyncStdinWriter ap) input)
      `finally` closeStdin ap
    snd <$> (sourceAsyncStdout ap $$ sinkHandle stdout)
              `concurrently` (sourceAsyncStderr ap $$ sinkHandle stderr)
              `concurrently` waitForProcess ap

runCommand :: (MonadBaseControl IO m, MonadIO m, MonadMask m)
           => Text -> [Text] -> ByteString -> ExecOptions -> ContainerT m ExitCode
runCommand = liftContainer4 runCommandIn

asyncStdinWriter :: AsyncProcess -> Maybe (ByteString -> IO ())
asyncStdinWriter TaskProc{..} = Nothing
asyncStdinWriter ap           = Just $ ahStdin $ apHandle ap

sinkAsyncProcess :: (MonadBaseControl IO m) => AsyncProcess -> Consumer ByteString m ()
sinkAsyncProcess TaskProc{..} = mempty
sinkAsyncProcess ap =
  mapM_C (liftBase . ahStdin (apHandle ap))

sourcePopper :: (MonadBaseControl IO m) => IO (Maybe ByteString) -> Producer m ByteString
sourcePopper prod = repeatWhileMC (liftBase prod) isJust .| concatC

sourceAsyncOutput :: (MonadCatch m, MonadBaseControl IO m, MonadIO m)
                  => AsyncProcess -> m (Source m ByteString)
sourceAsyncOutput TaskProc{..} = return $ return ()
sourceAsyncOutput InteractiveProc{..} = return $ sourcePopper $ ahOutput apHandle
sourceAsyncOutput p@ThreewayProc{} =
  runResourceT $ mergeSources [sourceAsyncStdout p, sourceAsyncStderr p] 20

sourceAsyncStdout :: (MonadBaseControl IO m) => AsyncProcess -> Producer m ByteString
sourceAsyncStdout ThreewayProc{..} = sourcePopper $ ahStdout apHandle
sourceAsyncStdout _                = return ()

sourceAsyncStderr :: (MonadBaseControl IO m) => AsyncProcess -> Producer m ByteString
sourceAsyncStderr ThreewayProc{..} = sourcePopper $ ahStderr apHandle
sourceAsyncStderr _                = return ()

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


data Permission = Permission { fileUID  :: Maybe UserID
                             , fileGID  :: Maybe GroupID
                             , fileMode:: Maybe FileMode
                             }
                deriving (Read, Show, Eq, Ord)
defaultPermission :: Permission
defaultPermission = Permission Nothing Nothing Nothing

instance Default Permission where
  def = defaultPermission

writeFileBody :: (MonadCatch m, MonadBaseControl IO m, MonadIO m) => FilePath -> Permission -> RequestBody -> ContainerT m Value
writeFileBody = liftContainer3 writeFileBodyIn

writeFileBodyIn :: (MonadBaseControl IO m, MonadIO m, MonadCatch m)
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

writeFileStrIn :: (MonadBaseControl IO m, MonadIO m, MonadCatch m)
            => Container -> FilePath -> Permission -> String -> LXDT m Value
writeFileStrIn c fp perm = writeFileBodyIn c fp perm . RequestBodyLBS . LBS.pack

writeFileLBSIn :: (MonadBaseControl IO m, MonadIO m, MonadCatch m)
            => Container -> FilePath -> Permission -> LBS.ByteString -> LXDT m Value
writeFileLBSIn c perm fp = writeFileBodyIn c perm fp . RequestBodyLBS

writeFileBSIn :: (MonadBaseControl IO m, MonadIO m, MonadCatch m)
              => Container -> FilePath -> Permission -> BS.ByteString -> LXDT m Value
writeFileBSIn c perm fp = writeFileBodyIn c perm fp . RequestBodyBS

writeFileStr :: (MonadCatch m, MonadBaseControl IO m, MonadIO m) => FilePath -> Permission -> String -> ContainerT m Value
writeFileStr = liftContainer3 writeFileStrIn

writeFileLBS :: (MonadCatch m, MonadBaseControl IO m, MonadIO m) => FilePath -> Permission -> LBS.ByteString -> ContainerT m Value
writeFileLBS = liftContainer3 writeFileLBSIn

writeFileBS :: (MonadCatch m, MonadBaseControl IO m, MonadIO m) => FilePath -> Permission -> BS.ByteString -> ContainerT m Value
writeFileBS = liftContainer3 writeFileBSIn

readAsyncProcessIn :: (MonadBaseControl IO m, MonadIO m, MonadMask m)
                   => Container -> Text -> [Text] -> ByteString
                   -> ExecOptions -> LXDT m (LBS.ByteString, LBS.ByteString, ExitCode)
readAsyncProcessIn c cmd args input opts = do
  bracket (executeIn c cmd args opts { execInteraction = Threeway })
          (liftBase . ahCloseProcess . apHandle) $ \ap -> do
    liftBase (fromJust (asyncStdinWriter ap) input) `finally` closeStdin ap
    runConcurrently $
      (,,) <$> Concurrently (sourceAsyncStdout ap $$ sinkLazy)
           <*> Concurrently (sourceAsyncStderr ap $$ sinkLazy)
           <*> Concurrently (waitForProcess ap)

readAsyncStdout :: AsyncProcess -> IO (Maybe ByteString)
readAsyncStdout ThreewayProc{..} = ahStdout apHandle
readAsyncStdout _                = return Nothing

readAsyncStderr :: AsyncProcess -> IO (Maybe ByteString)
readAsyncStderr ThreewayProc{..} = ahStderr apHandle
readAsyncStderr _                = return Nothing

readAsyncOutput :: AsyncProcess -> IO (Maybe ByteString)
readAsyncOutput InteractiveProc{..} = ahOutput apHandle
readAsyncOutput _                   = return Nothing

readAsyncProcess :: (MonadMask m, MonadBaseControl IO m, MonadIO m)
                 => Text -> [Text] -> ByteString -> ExecOptions
                 -> ContainerT m (LBS.ByteString, LBS.ByteString, ExitCode)
readAsyncProcess = liftContainer4 readAsyncProcessIn

readFileOrListDirFrom :: (MonadCatch m, MonadBaseControl IO m, MonadIO m)
                      => Container -> FilePath -> LXDT m (Either [FilePath] String)
readFileOrListDirFrom c fp = do
  v <- fromSync =<< get (fileEndPoint c fp)
  case v of
    AE.Array{}  | AE.Success val <- AE.fromJSON v -> return $ Left val
    AE.String{} | AE.Success val <- AE.fromJSON v -> return $ Right val
    _ -> throwM $ MalformedResponse (fileEndPoint c fp) $
         "File list or string expected, but got: " <> LBS.unpack (encode v)

closeProcessIO :: (MonadBaseControl IO m) => AsyncProcess -> LXDT m ()
closeProcessIO TaskProc{} = return ()
closeProcessIO ap         = liftBase $ ahCloseProcess $ apHandle ap

data ContainerAction = Stop     { actTimeout  :: Int
                                , actStateful :: Bool
                                , actForce    :: Bool
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

fromAsync :: (MonadMask m, MonadBaseControl IO m, MonadIO m)
           => LXDT m (LXDResult OpToAsync) -> LXDT m AsyncProcess
fromAsync act = act >>= \case
  LXDError{..} ->
    throwM $ ServerError lxdErrorCode $
    unlines [T.unpack lxdErrorMessage, LBS.unpack (encode lxdErrorMetadata)]
  LXDSync{} ->
    throwM $ MalformedResponse "" "Synchronous result returned instead of asynchronous"

  LXDAsync{..} | OpToAsync p <- lxdAsyncMetadata -> do
    ap0 <- liftBase $ p (Operation lxdAsyncOperation) lxdAsyncExitCode
    mah <- getAsyncHandle ap0
    return $ maybe ap0 (\ah -> ap0 { apHandle = ah { ahCloseProcess = ahCloseProcess ah >> killThread lxdAsyncWaiter} })
               mah

setContainerState :: (MonadMask m, MonadBaseControl IO m, MonadIO m)
                  => Container -> ContainerAction -> LXDT m Bool
setContainerState c cs = do
  ap <- fromAsync $
    request (\q -> q { method = "PUT"
                     , requestBody = RequestBodyLBS $ encode cs })
      ("/1.0/containers/" <> T.unpack c <> "/state")
  ans <- waitForOperationTimeout Nothing ap
  return $ ans == Just ExitSuccess

setState :: (MonadMask m, MonadBaseControl IO m, MonadIO m)
         => ContainerAction -> ContainerT m Bool
setState = liftContainer setContainerState

startContainer :: (MonadMask m, MonadBaseControl IO m, MonadIO m)
               => Container
               -> Int           -- ^ timeout
               -> Bool          -- ^ is stateful?
               -> LXDT m Bool   -- ^ successful?
startContainer c wait st = setContainerState c (Start wait st)

start :: (MonadMask m, MonadBaseControl IO m, MonadIO m) => Int -> Bool -> ContainerT m Bool
start = liftContainer2 startContainer

stopContainer :: (MonadMask m, MonadBaseControl IO m, MonadIO m)
              => Container
              -> Int           -- ^ timeout
              -> Bool          -- ^ is stateful?
              -> LXDT m Bool
stopContainer c wait st =
  setContainerState c Stop { actTimeout  = wait
                           , actForce    = False
                           , actStateful = st
                           }

stop :: (MonadMask m, MonadBaseControl IO m, MonadIO m) => Int -> Bool -> ContainerT m Bool
stop = liftContainer2 stopContainer

getContainerInfo :: (MonadIO m, MonadBaseControl IO m, MonadCatch m)
                 => Container -> LXDT m ContainerInfo
getContainerInfo c =
  fromSync =<< get ("/1.0/containers/" <> T.unpack c)

putContainerInfo :: (MonadIO m, MonadMask m, MonadBaseControl IO m)
                 => Container -> ContainerInfo -> LXDT m Bool
putContainerInfo c info = do
  ap <- fromAsync $
    request (\q -> q { method = "PUT"
                     , requestBody = RequestBodyLBS $ encode info })
      ("/1.0/containers/" <> T.unpack c)
  ans <- waitForOperationTimeout Nothing ap
  return $ ans == Just ExitSuccess

killContainer :: (MonadIO m, MonadMask m, MonadBaseControl IO m)
              => Container
              -> Int           -- ^ timeout
              -> Bool          -- ^ is stateful?
              -> LXDT m Bool
killContainer c wait st =
  setContainerState c Stop { actTimeout  = wait
                           , actForce    = True
                           , actStateful = st
                           }

kill :: (MonadMask m, MonadBaseControl IO m, MonadIO m) => Int -> Bool -> ContainerT m Bool
kill = liftContainer2 killContainer

addCertificate :: (MonadCatch m, MonadIO m, MonadBaseControl IO m)
               => Maybe String -> ByteString -> Maybe String -> LXDT m ()
addCertificate mname cert mpass =
  let val = object $ concat
            [ ["type" .= "client"
              ,"certificate" .= T.decodeUtf8 cert
              ]
            , foldMap (return . ("name" .=)) mname
            , foldMap (return . ("password" .=)) mpass
            ]
  in expectNull "/1.0/certificates" =<< fromSync =<< post "/1.0/certificates" val

asValue :: Value -> Value
asValue = id

newtype Operation = Operation { runOperation :: String }
               deriving (Read, Show, Eq, Ord)
