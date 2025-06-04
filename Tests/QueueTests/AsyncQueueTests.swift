import Testing
@testable import Queue

@Suite(.timeLimit(.minutes(1)))
struct AsyncQueueTests {
    
    // MARK: - Value and Error
    
    @Test
    func perform_returns_nonthrowing_operation_value() async throws {
        let queue = AsyncQueue()
        
        for value in 0..<10 {
            let result = await queue.perform {
                return value
            }
            #expect(result == value)
        }
    }
    
    @Test
    func perform_rethrows_thrown_error() async throws {
        struct TestError: Error { }
        let queue = AsyncQueue()
        
        await #expect(throws: TestError.self) {
            try await queue.perform {
                throw TestError()
            }
        }
    }
    
    @Test
    func performUnlessCancelled_returns_nonthrowing_operation_value() async throws {
        let queue = AsyncQueue()
        
        for value in 0..<10 {
            let result = try await queue.performUnlessCancelled {
                return value
            }
            #expect(result == value)
        }
    }
    
    @Test
    func performUnlessCancelled_rethrows_thrown_error() async throws {
        struct TestError: Error { }
        let queue = AsyncQueue()
        
        await #expect(throws: TestError.self) {
            try await queue.performUnlessCancelled {
                throw TestError()
            }
        }
    }
    
    @Test
    func added_task_returns_nonthrowing_operation_value() async throws {
        let queue = AsyncQueue()
        
        for value in 0..<10 {
            let task = queue.addTask {
                return value
            }
            let result = await task.value
            #expect(result == value)
        }
    }
    
    @Test
    func added_task_rethrows_thrown_error() async throws {
        struct TestError: Error { }
        let queue = AsyncQueue()
        
        let task = queue.addTask {
            throw TestError()
        }
        
        await #expect(throws: TestError.self) {
            try await task.value
        }
    }
    
    @Test
    func discardable_task_returns_operation_value() async throws {
        let queue = AsyncQueue()
        
        for value in 0..<10 {
            let task = queue.addDiscardableTask {
                return value
            }
            let result = try await task.value
            #expect(result == value)
        }
    }
    
    @Test
    func discardable_task_rethrows_thrown_error() async throws {
        struct TestError: Error { }
        let queue = AsyncQueue()
        
        let task = queue.addDiscardableTask {
            throw TestError()
        }
        
        await #expect(throws: TestError.self) {
            try await task.value
        }
    }
    
    // MARK: - Ordering
    
    @Test
    func nonthrowing_tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = AsyncQueue()
        for i in 0...10000 {
            queue.addTask {
                valuesMutex.withLock { $0.append(i) }
            }
        }
        let values = await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10000))
    }
    
    @Test
    func cancelled_nonthrowing_tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = AsyncQueue()
        for i in 0...10000 {
            let task = queue.addTask {
                valuesMutex.withLock { $0.append(i) }
            }
            task.cancel()
        }
        let values = await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10000))
    }
    
    @Test
    func throwing_tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = AsyncQueue()
        func handleValue(_ value: Int) throws {
            valuesMutex.withLock { $0.append(value) }
        }
        for i in 0...10000 {
            queue.addTask {
                try handleValue(i)
            }
        }
        let values = await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10000))
    }
    
    @Test
    func cancelled_throwing_tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = AsyncQueue()
        func handleValue(_ value: Int) throws {
            valuesMutex.withLock { $0.append(value) }
        }
        for i in 0...10000 {
            let task = queue.addTask {
                try handleValue(i)
            }
            task.cancel()
        }
        let values = await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10000))
    }
    
    @Test
    func discardable_tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = AsyncQueue()
        for i in 0...10000 {
            queue.addDiscardableTask {
                valuesMutex.withLock { $0.append(i) }
            }
        }
        let values = await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10000))
    }
    
    @Test
    func cancelled_discardable_tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = AsyncQueue()
        for i in 0...10000 {
            let task = queue.addDiscardableTask {
                valuesMutex.withLock { $0.append(i) }
            }
            if Bool.random() {
                task.cancel()
            }
        }
        let values = await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == values.sorted())
    }
    
    // MARK: - Cancellation
    
    @Test
    func perform_nonthrowing_operation_is_cancelled_by_late_wrapper_task_cancellation() async throws {
        let queue = AsyncQueue()
        
        // Semaphore-like stream: signal is `continuation.finish()`,
        // wait is `for await _ in stream { }`
        let (didStartStream, didStartContinuation) = AsyncStream.makeStream(of: Never.self)
        
        let wrapperTask = Task {
            await queue.perform {
                didStartContinuation.finish()
                
                // Wait until task is cancelled.
                let didCancelStream = AsyncStream<Never> { _ in }
                for await _ in didCancelStream { }
                #expect(Task.isCancelled)
            }
        }
        
        for await _ in didStartStream { }
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    @Test
    func perform_throwing_operation_is_cancelled_by_late_wrapper_task_cancellation() async throws {
        let queue = AsyncQueue()
        
        // Semaphore-like stream: signal is `continuation.finish()`,
        // wait is `for await _ in stream { }`
        let (didStartStream, didStartContinuation) = AsyncStream.makeStream(of: Never.self)
        
        let wrapperTask = Task {
            _ = await #expect(throws: CancellationError.self) {
                try await queue.perform {
                    didStartContinuation.finish()
                    
                    // Wait until task is cancelled.
                    let didCancelStream = AsyncStream<Never> { _ in }
                    for await _ in didCancelStream { }
                    try Task.checkCancellation()
                }
            }
        }
        
        for await _ in didStartStream { }
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    @Test
    func perform_nonthrowing_operation_is_cancelled_by_early_wrapper_task_cancellation() async throws {
        let queue = AsyncQueue()
        
        let wrapperTask = Task {
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            
            await queue.perform {
                #expect(Task.isCancelled)
            }
        }
        
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    @Test
    func perform_throwing_operation_is_cancelled_by_early_wrapper_task_cancellation() async throws {
        let queue = AsyncQueue()
        
        let wrapperTask = Task {
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            
            await #expect(throws: CancellationError.self) {
                try await queue.perform {
                    try Task.checkCancellation()
                }
            }
        }
        
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    @Test
    func performUnlessCancelled_operation_is_cancelled_by_late_wrapper_task_cancellation() async throws {
        let queue = AsyncQueue()
        
        // Semaphore-like stream: signal is `continuation.finish()`,
        // wait is `for await _ in stream { }`
        let (didStartStream, didStartContinuation) = AsyncStream.makeStream(of: Never.self)
        
        let wrapperTask = Task {
            try await queue.performUnlessCancelled {
                didStartContinuation.finish()
                
                // Wait until task is cancelled.
                let didCancelStream = AsyncStream<Never> { _ in }
                for await _ in didCancelStream { }
                #expect(Task.isCancelled)
            }
        }
        
        for await _ in didStartStream { }
        wrapperTask.cancel()
        try await wrapperTask.value
    }
    
    @Test
    func performUnlessCancelled_operation_is_cancelled_by_early_wrapper_task_cancellation() async throws {
        let queue = AsyncQueue()
        
        let wrapperTask = Task {
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            
            await #expect(throws: CancellationError.self) {
                try await queue.performUnlessCancelled {
                    Issue.record("Operation should not run")
                }
            }
        }
        
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    @Test
    func cancelled_discardable_task_does_not_wait_for_previous_operations() async throws {
        let queue = AsyncQueue()
        
        // GIVEN a first task that does not end until the test is complete.
        let (firstTaskStream, firstTaskContinuation) = AsyncStream.makeStream(of: Never.self)
        defer { firstTaskContinuation.finish() }
        queue.addTask {
            for await _ in firstTaskStream { }
        }
        
        // GIVEN a discardable task enqueued after the first task.
        let discardableTask = queue.addDiscardableTask {
            Issue.record("Operation should not run")
        }
        
        // WHEN the discardable task is cancelled,
        discardableTask.cancel()
        
        // THEN it completes with `CancellationError` before the first task has ended.
        await #expect(throws: CancellationError.self) {
            try await discardableTask.value
        }
    }
    
    @Test
    func cancelled_performUnlessCancelled_does_not_wait_for_previous_operations() async throws {
        let queue = AsyncQueue()
        
        let (firstTaskStream, firstTaskContinuation) = AsyncStream.makeStream(of: Never.self)
        queue.addTask {
            for await _ in firstTaskStream { }
        }
        
        let wrapperTask = Task {
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            
            await #expect(throws: CancellationError.self) {
                try await queue.performUnlessCancelled {
                    Issue.record("Operation should not run")
                }
            }
            
            firstTaskContinuation.finish()
            
            await queue.perform {
                #expect(Task.isCancelled)
            }
        }
        
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    // MARK: - Isolation
    
    @Test
    func perform_closure_inherits_isolation() {
        // This test passes if it compiles without any error or warning.
        @MainActor class MyClass {
            let queue = AsyncQueue()
            
            private func enqueue() async {
                await queue.perform {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
        
        actor MyActor {
            let queue = AsyncQueue()
            
            private func enqueue() async {
                await queue.perform {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
    }
    
    @Test
    func performUnlessCancelled_closure_inherits_isolation() {
        // This test passes if it compiles without any error or warning.
        @MainActor class MyClass {
            let queue = AsyncQueue()
            
            private func enqueue() async throws {
                try await queue.performUnlessCancelled {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
        
        actor MyActor {
            let queue = AsyncQueue()
            
            private func enqueue() async throws {
                try await queue.performUnlessCancelled {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
    }
    
    @Test
    func addTask_closure_inherits_isolation() {
        // This test passes if it compiles without any error or warning.
        @MainActor class MyClass {
            let queue = AsyncQueue()
            
            private func enqueue() -> Task<Void, Never> {
                queue.addTask {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
        
        actor MyActor {
            let queue = AsyncQueue()
            
            private func enqueue() -> Task<Void, Never> {
                queue.addTask {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
    }
    
    @Test
    func addDiscardableTask_closure_inherits_isolation() {
        // This test passes if it compiles without any error or warning.
        @MainActor class MyClass {
            let queue = AsyncQueue()
            
            private func enqueue() -> Task<Void, any Error> {
                queue.addDiscardableTask {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
        
        actor MyActor {
            let queue = AsyncQueue()
            
            private func enqueue() -> Task<Void, any Error> {
                queue.addDiscardableTask {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
    }
}
