package com.epam.reportportal.migration.seek;

import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public abstract class AbstractObjectIdItemStreamItemReader<T> extends AbstractItemStreamItemReader<T> {

	private static final String OBJECT_ID = "object.id";
	private static final String OBJECT_LATEST_ID = "object.latest.id";

	private ObjectId latestObjectId;

	private ObjectId currentObjectId;

	/**
	 * Read next item from input.
	 *
	 * @return item
	 * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
	 */
	protected abstract T doRead() throws Exception;

	/**
	 * Open resources necessary to start reading input.
	 *
	 * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
	 */
	protected abstract void doOpen() throws Exception;

	/**
	 * Close the resources opened in {@link #doOpen()}.
	 *
	 * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
	 */
	protected abstract void doClose() throws Exception;

	/**
	 * Move to the given item index. Subclasses should override this method if
	 * there is a more efficient way of moving to given index than re-reading
	 * the input using {@link #doRead()}.
	 *
	 * @param itemIndex index of item (0 based) to jump to.
	 * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
	 */
	protected void jumpToItem(ObjectId objectId) throws Exception {
		return;
	}

	public void setLatestObjectId(ObjectId latestObjectId) {
		this.latestObjectId = latestObjectId;
	}

	public void setCurrentObjectId(ObjectId currentObjectId) {
		this.currentObjectId = currentObjectId;
	}

	public ObjectId getCurrentObjectId() {
		return currentObjectId;
	}

	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException {
		if (latestObjectId.equals(currentObjectId)) {
			return null;
		}
		T t = doRead();
		if (t instanceof DBObject) {
			currentObjectId = (ObjectId) ((DBObject) t).get("_id");
		}
		return t;
	}

	@Override
	public void close() throws ItemStreamException {
		super.close();
		currentObjectId = null;
		try {
			doClose();
		} catch (Exception e) {
			throw new ItemStreamException("Error while closing item reader", e);
		}
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		try {
			doOpen();
		} catch (Exception e) {
			throw new ItemStreamException("Failed to initialize the reader", e);
		}

		if (executionContext.containsKey(getExecutionContextKey(OBJECT_LATEST_ID))) {
			latestObjectId = (ObjectId) executionContext.get(getExecutionContextKey(OBJECT_LATEST_ID));
		}

		ObjectId objectId = null;
		if (executionContext.containsKey(getExecutionContextKey(OBJECT_ID))) {
			objectId = (ObjectId) executionContext.get(getExecutionContextKey(OBJECT_ID));
		} else if (currentObjectId != null) {
			objectId = currentObjectId;
		}

		if (objectId != null && objectId != latestObjectId) {
			try {
				jumpToItem(objectId);
			} catch (Exception e) {
				throw new ItemStreamException("Could not move to stored position on restart", e);
			}
		}

		currentObjectId = objectId;

	}

}
