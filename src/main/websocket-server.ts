import { WebSocketServer, WebSocket } from 'ws';
import os from 'os';
import { spawn } from 'child_process';

const workerExecutablePath = '/RaceHubWorker.exe';

interface WorkerRecord {
  id: string;
  ws: WebSocket;
  workerType: 'recorder' | 'parser' | 'post_processor';
}

interface RecorderWorkerRecord extends WorkerRecord {
  recorderId: string;
  recorderType: string;
  inPort: number;
  outAddress: string;
  outPort: number;
  forwardingEnabled: boolean;
}

const workers: Map<string, RecorderWorkerRecord> = new Map();

const generateUniqueWorkerId = (): string => {
  // return `worker-${Date.now()}`;
  return '1';
};

const sendMessage = (
  ws: WebSocket,
  action: string,
  payload: Record<string, any>,
) => {
  const message = JSON.stringify({ action, payload });
  ws.send(message);
};

const spawnWorker = (workerId: string): Promise<void> => {
  return new Promise((resolve, reject) => {
    const process = spawn(workerExecutablePath, [
      '--server',
      'ws://localhost:8082',
      '--workerId',
      workerId,
    ]);

    process.on('error', (error) => {
      console.error('Failed to start the process:', error);
      reject(error);
    });

    process.stdout.on('data', (data) => {
      console.log(`stdout: ${data}`);
    });

    process.stderr.on('data', (data) => {
      console.error(`stderr: ${data.toString()}`);
    });

    process.on('close', (code) => {
      console.log(`Worker process exited with code ${code}`);
      resolve();
    });
  });
};

const handleStartRecording = async (ws: WebSocket, payload: any) => {
  const {
    recorderId,
    recorderType,
    inPort,
    outAddress,
    outPort,
    forwardingEnabled,
  } = payload;
  const workerId = generateUniqueWorkerId();

  try {
    // await spawnWorker(workerId);
    workers.set(workerId, {
      id: workerId,
      ws,
      workerType: 'recorder',
      recorderId,
      recorderType,
      inPort,
      outAddress,
      outPort,
      forwardingEnabled,
    });
    console.log(`Worker ${workerId} started and registered.`);
  } catch (error) {
    console.error(`Failed to start worker ${workerId}:`, error);
    sendMessage(ws, 'ERROR', {
      message: `Failed to start worker: ${error.message}`,
    });
  }
};

const handleStopRecording = async (ws: WebSocket, payload: any) => {
  const { workerId } = payload;
  const worker = workers.get(workerId);
  if (worker) {
    sendMessage(ws, 'WORKER_REQUEST_TERMINATE', { workerId });
  } else {
    console.error(`Worker ${workerId} not found.`);
  }
};

const handleWorkerResponseReady = async (ws: WebSocket, payload: any) => {
  const { workerId } = payload;
  const worker = workers.get(workerId);
  if (worker && worker.workerType === 'recorder') {
    const {
      recorderId,
      recorderType,
      inPort,
      outAddress,
      outPort,
      forwardingEnabled,
    } = worker;
    sendMessage(ws, 'WORKER_CONFIG', {
      workerType: 'recorder',
      recorderId,
      recorderType,
      inPort,
      outAddress,
      outPort,
      forwardingEnabled,
    });
  } else {
    console.error(`Worker ${workerId} is not a recorder or not found.`);
  }
};

const handleWorkerTerminated = (payload: any) => {
  const { workerId } = payload;
  if (workers.delete(workerId)) {
    console.log(`Worker ${workerId} terminated and unregistered.`);
  } else {
    console.error(`Worker ${workerId} not found.`);
  }
};

const handleUnknownAction = (ws: WebSocket, action: string) => {
  console.log(`Unknown action: ${action}`);
  sendMessage(ws, 'ERROR', { message: `Unknown action: ${action}` });
};

const sendInitialMessage = (ws: WebSocket) => {
  const payload = {
    versionMajor: 0,
    versionMinor: 1,
    versionPatch: 1,
    platform: os.platform(),
    hostname: os.hostname(),
  };
  sendMessage(ws, 'CLIENT_INITIALISED', payload);
};

const handleMessages = async (
  ws: WebSocket,
  message: string | Buffer | ArrayBuffer | Buffer[],
) => {
  let parsedMessage;

  if (Buffer.isBuffer(message) || message instanceof ArrayBuffer) {
    message = message.toString();
  }

  try {
    parsedMessage = JSON.parse(message as string);
  } catch (error) {
    console.error('Failed to parse message:', message);
    return;
  }

  const { action, payload } = parsedMessage;

  switch (action) {
    case 'START_RECORDING':
      await handleStartRecording(ws, payload);
      break;

    case 'STOP_RECORDING':
      await handleStopRecording(ws, payload);
      break;

    case 'WORKER_RESPONSE_READY':
      await handleWorkerResponseReady(ws, payload);
      break;

    case 'WORKER_TERMINATED':
      handleWorkerTerminated(payload);
      break;

    default:
      handleUnknownAction(ws, action);
  }
};

const startWebSocketServer = () => {
  const wss = new WebSocketServer({ port: 8082 });

  wss.on('connection', (ws: WebSocket) => {
    sendInitialMessage(ws);

    ws.on('message', async (message) => {
      await handleMessages(ws, message);
    });

    ws.on('close', () => {
      console.log('Client disconnected');
    });
  });

  console.log('WebSocket server started on ws://localhost:8082');
};

export default startWebSocketServer;
