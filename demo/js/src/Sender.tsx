import * as React from 'react';
import { useState } from 'react';

interface SenderProps {
  group: string;
}

type MessageInputModel = {
  message: string;
};

export function Sender({ group }: SenderProps) {
  const [input, setInput] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [status, setStatus] = useState('editing message');

  async function handleSubmit(ev: React.FormEvent<HTMLFormElement>) {
    ev.preventDefault();
    setIsSubmitting(true);
    setStatus('sending...');
    const url = new URL('/api/chat/send', window.location.origin);
    url.searchParams.append('group', group);
    const msg: MessageInputModel = {
      message: input,
    };
    try {
      const resp = await fetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
        },
        body: JSON.stringify(msg),
      });
      if (resp.status != 200) {
        setStatus(`error, unexpected status ${resp.status}`);
        return;
      }
      setStatus('sent');
      setInput('');
    } catch (err) {
      setStatus(`error, ${err.message}`);
    }
    setIsSubmitting(false);
  }

  function handleChange(ev: React.FormEvent<HTMLInputElement>) {
    const value = ev.currentTarget.value;
    setInput(value);
    setStatus('editing message');
  }

  return (
    <div>
      <form onSubmit={handleSubmit}>
        <fieldset disabled={isSubmitting}>
          <input type="text" value={input} onChange={handleChange} />
          <p>Status: {status} </p>
        </fieldset>
      </form>
    </div>
  );
}
