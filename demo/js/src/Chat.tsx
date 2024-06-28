import * as React from 'react';
import { useState } from 'react';

import { Listener } from './Listener';
import { Sender } from './Sender';

type SystemOutputModel = {
    topic: string;
};

export function Chat() {
  const [group, setGroup] = useState('');
  const [enteredChat, setEnteredChat] = useState(false);
  const [isSystemTopic, setIsSystemTopic] = useState(false);

  const joinChat = async () => {
      setEnteredChat(true);
  };

  const monitorSystem = async () => {
      const url = new URL('/api/chat/system', window.location.origin);
      const response = await fetch(url, {
          method: 'GET',
          headers: {
              'Content-Type': 'application/json'
          }
      });
      const system: SystemOutputModel = await response.json();

      setGroup(system.topic);
      setIsSystemTopic(true);
      setEnteredChat(true);
  };

  if (enteredChat) {
    return (
      isSystemTopic ? <Listener group={group} /> : <>
        <Listener group={group} />
        <Sender group={group} />
      </>
    );
  }

  return (
    <div>
        <div>
            <input type="text" value={group} onChange={e => setGroup(e.target.value)} placeholder="Type your group "/>
            <button onClick={joinChat}>Join Chat</button>
        </div>
        <div>
            <div style={{marginBottom: '30px'}}></div>
        </div>
        <div>
            <button onClick={monitorSystem}>Monitor System</button>
        </div>
    </div>
  );
}
