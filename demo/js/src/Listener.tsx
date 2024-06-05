import * as React from 'react';
import { useEffect, useState } from 'react';

const MAX_MESSAGES = 15;

function appendLine(lines: Array<string>, line: string): Array<string> {
  const res = [...lines, line];
  if (res.length > MAX_MESSAGES) {
    res.shift();
  }
  return res;
}

type MessageOutputModel = {
  message: string;
  subscribedNode?: string;
  publishingNode?: string;
};

function eventSourceStatus(eventSource: EventSource) {
  switch (eventSource.readyState) {
    case EventSource.CLOSED:
      return 'closed';
    case EventSource.CONNECTING:
      return 'connecting...';
    case EventSource.OPEN:
      return 'connected';
    default:
      return `unknown (${eventSource.readyState})`;
  }
}

interface ListenerProps {
  group: string;
}

export function Listener({ group }: ListenerProps) {
  const [outputLines, setOutputLines] = useState([]);
  const [status, setStatus] = useState('disconnected');

  useEffect(() => {
    console.log(`creating new EventSource`);
    const url = new URL('/api/chat/listen', window.location.origin);
    url.searchParams.append('group', group);
    const eventSource = new EventSource(url);
    setStatus('connecting...');
    eventSource.addEventListener('error', () => {
      setStatus(eventSourceStatus(eventSource));
    });

    eventSource.addEventListener(group, ev => {
      const data: MessageOutputModel = JSON.parse(ev.data);
      const line = (data.subscribedNode != null && data.publishingNode != null) ?
          "[PUBLISHING NODE " + data.publishingNode + "] -> [SUBSCRIBED NODE " + data.subscribedNode + "] " + data.message
      : data.message;

      setStatus(eventSourceStatus(eventSource));
      setOutputLines(outputLines => appendLine(
          outputLines,
          line
      ));
    });

    eventSource.addEventListener('open', () => {
      setStatus(eventSourceStatus(eventSource));
    });

    return () => {
      eventSource.close();
    };
  }, [group]);

  return (
    <div>
      <h3>{group}</h3>
      <textarea value={outputLines.join('\n')} cols={100} rows={MAX_MESSAGES + 2} readOnly={true} />
      <p>Status: {status} </p>
    </div>
  );
}
