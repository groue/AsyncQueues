/// A type that helps dealing with task cancellation.
///
/// Usage:
///
/// ```swift
/// let cancellationMutex = Mutex(TaskCancellationState.notStarted)
/// let result = try await withTaskCancellationHandler {
///     let task = cancellationMutex.startTask {
///         // return a Task
///     }
///
///     return /* result */
/// } onCancel: {
///     cancellationMutex.cancel()
/// }
/// ```
enum TaskCancellationState {
    case notStarted
    case cancellable(@Sendable () -> Void)
    case cancelled
}

extension Mutex<TaskCancellationState> {
    func startTask<Success, Failure>(
        _ makeTask: () -> Task<Success, Failure>
    ) -> Task<Success, Failure> {
        let task: Task<Success, Failure>
        let isCancelled: Bool
        (task, isCancelled) = withLock { state in
            let task = makeTask()
            
            if case .cancelled = state {
                return (task, true)
            } else {
                state = .cancellable(task.cancel)
                return (task, false)
            }
        }
        
        if isCancelled {
            task.cancel()
        }
        
        return task
    }
    
    func cancel() {
        let cancel: (@Sendable () -> Void)?
        cancel = withLock { state in
            defer {
                state = .cancelled
            }
            if case .cancellable(let cancel) = state {
                return cancel
            } else {
                return nil
            }
        }
        
        cancel?()
    }
}
