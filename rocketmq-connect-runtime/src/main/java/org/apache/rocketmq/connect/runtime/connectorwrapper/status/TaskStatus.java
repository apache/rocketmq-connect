package org.apache.rocketmq.connect.runtime.connectorwrapper.status;

import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;

/**
 * task state
 */
public class TaskStatus extends AbstractStatus<ConnectorTaskId> {
    public TaskStatus(){}
    public TaskStatus(ConnectorTaskId id, State state, String workerId, Long generation) {
        super(id, state, workerId, generation, null);
    }

    public TaskStatus(ConnectorTaskId id, State state, String workerId, Long generation, String trace) {
        super(id, state, workerId, generation, trace);
    }

    public interface Listener {

        /**
         * Invoked after successful startup of the task.
         * @param id The id of the task
         */
        void onStartup(ConnectorTaskId id);

        /**
         * Invoked after the task has been paused.
         * @param id The id of the task
         */
        void onPause(ConnectorTaskId id);

        /**
         * Invoked after the task has been resumed.
         * @param id The id of the task
         */
        void onResume(ConnectorTaskId id);

        /**
         * Invoked if the task raises an error. No shutdown event will follow.
         * @param id The id of the task
         * @param cause The error raised by the task.
         */
        void onFailure(ConnectorTaskId id, Throwable cause);

        /**
         * Invoked after successful shutdown of the task.
         * @param id The id of the task
         */
        void onShutdown(ConnectorTaskId id);

        /**
         * Invoked after the task has been deleted. Can be called if the
         * connector tasks have been reduced, or if the connector itself has
         * been deleted.
         * @param id The id of the task
         */
        void onDeletion(ConnectorTaskId id);

    }

}
