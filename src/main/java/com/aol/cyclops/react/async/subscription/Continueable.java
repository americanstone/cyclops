package com.aol.cyclops.react.async.subscription;

import com.aol.cyclops.data.async.Queue;

public interface Continueable {

	public void closeQueueIfFinished(Queue queue);

	public void addQueue(Queue queue);
	public void registerSkip(long skip);
	public void registerLimit(long limit);
	public void closeAll(Queue q);

	public boolean closed();

	public void closeQueueIfFinishedStateless(Queue queue);

	public void closeAll();

	public long timeLimit();
	public void registerTimeLimit(long nanos);
}
