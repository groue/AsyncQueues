/// A queue that serializes tasks.
final class PrimitiveAsyncQueue: Sendable {
    typealias Operation = @Sendable () async -> Void
    
    private let queueContinuation: AsyncStream<Operation>.Continuation
    
    init() {
        // Execute, in order, all operations submitted to `queueContinuation`.
        let (queueStream, queueContinuation) = AsyncStream.makeStream(of: Operation.self)
        self.queueContinuation = queueContinuation
        Task {
            for await operation in queueStream {
                await operation()
            }
        }
    }
    
    deinit {
        queueContinuation.finish()
    }
    
    func makeSemaphores() -> (start: Semaphore, end: Semaphore) {
        let start = Semaphore()
        let end = Semaphore()
        
        // Enqueue a closure that signals `start` and waits for `end`
        queueContinuation.yield {
            start.signal()
            await end.wait()
        }
        
        return (start: start, end: end)
    }
}
