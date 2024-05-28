import * as React from 'react';
import { useState } from 'react';

import { Listener } from './Listener';
import { Sender } from './Sender';

export function Chat() {
  const [group, setGroup] = useState('');
  const [enteredChat, setEnteredChat] = useState(false);

  if (enteredChat) {
    return (
      <>
        <Listener group={group} />
        <Sender group={group} />
      </>
    );
  }

  return (
    <div>
      <input type="text" value={group} onChange={e => setGroup(e.target.value)} placeholder="Type your group " />
      <button onClick={() => setEnteredChat(true)}>Join Chat</button>
    </div>
  );
}
