import AsyncQueues
import Testing

@Suite(.timeLimit(.minutes(1)))
struct DiscardingAsyncQueueTests {
    @TaskLocal static var local: Int? = nil
    
    // MARK: - Current Task
    
    @Test
    func perform_executes_operation_in_the_current_task() async throws {
        let queue = DiscardingAsyncQueue()
        
        let task = Task {
            try await queue.perform {
                withUnsafeCurrentTask { currentTask in
                    currentTask?.cancel()
                }
                #expect(Task.isCancelled)
            }
            #expect(Task.isCancelled)
        }
        
        try await task.value
    }
    
    @Test
    func perform_inherits_task_locals() async throws {
        let queue = DiscardingAsyncQueue()
        
        let number = Int.random(in: 0..<1000)
        try await Self.$local.withValue(number) {
            try await queue.perform {
                #expect(Self.local == number)
            }
        }
    }
    
    // MARK: - Value and Error
    
    @Test
    func perform_returns_operation_value() async throws {
        let queue = DiscardingAsyncQueue()
        
        for value in 0..<10 {
            let result = try await queue.perform {
                return value
            }
            #expect(result == value)
        }
    }
    
    @Test
    func perform_returns_non_sendable_value() async throws {
        class NonSendable { }
        
        @globalActor actor MyGlobalActor {
            static let shared = MyGlobalActor()
        }
        
        let queue = DiscardingAsyncQueue()
        
        do {
            let value = NonSendable()
            let result = try await queue.perform {
                return value
            }
            #expect(result === value)
        }
        
        // Won't compile.
        //
        // do {
        //     let value = NonSendable()
        //     let result = try await queue.perform { @MyGlobalActor in
        //         return value
        //     }
        //     #expect(result === value)
        // }
        
        do {
            let _ = try await queue.perform {
                return NonSendable()
            }
        }
        
        do {
            let _ = try await queue.perform { @MyGlobalActor in
                return NonSendable()
            }
        }
    }
    
    @Test
    func perform_rethrows_thrown_error() async throws {
        struct TestError: Error { }
        let queue = DiscardingAsyncQueue()
        
        await #expect(throws: TestError.self) {
            try await queue.perform {
                throw TestError()
            }
        }
    }
    
    @Test
    func added_task_returns_operation_value() async throws {
        let queue = DiscardingAsyncQueue()
        
        for value in 0..<10 {
            let task = queue.addTask {
                return value
            }
            let result = try await task.value
            #expect(result == value)
        }
    }
    
    @Test
    func added_task_rethrows_thrown_error() async throws {
        struct TestError: Error { }
        let queue = DiscardingAsyncQueue()
        
        let task = queue.addTask {
            throw TestError()
        }
        
        await #expect(throws: TestError.self) {
            try await task.value
        }
    }
    
    // MARK: - Ordering
    
    @Test
    func perform_is_ordered_with_task() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = DiscardingAsyncQueue()
        for i in 0...5000 {
            queue.addTask {
                valuesMutex.withLock { $0.append(i * 2) }
            }
            try await queue.perform {
                valuesMutex.withLock { $0.append(i * 2 + 1) }
            }
        }
        let values = try await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10001))
    }
    
    @Test
    func tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = DiscardingAsyncQueue()
        for i in 0...10000 {
            queue.addTask {
                valuesMutex.withLock { $0.append(i) }
            }
        }
        let values = try await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10000))
    }
    
    @Test
    func cancelled_tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = DiscardingAsyncQueue()
        for i in 0...10000 {
            let task = queue.addTask {
                valuesMutex.withLock { $0.append(i) }
            }
            if Bool.random() {
                task.cancel()
            }
        }
        let values = try await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values.count > 1000)
        #expect(values == values.sorted())
    }
    
    // MARK: - Cancellation
    
    @Test
    func operation_is_cancelled_by_late_wrapper_task_cancellation() async throws {
        let queue = DiscardingAsyncQueue()
        
        // Semaphore-like stream: signal is `continuation.finish()`,
        // wait is `for await _ in stream { }`
        let (didStartStream, didStartContinuation) = AsyncStream.makeStream(of: Never.self)
        
        let wrapperTask = Task {
            try await queue.perform {
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
    func operation_is_cancelled_by_early_wrapper_task_cancellation() async throws {
        let queue = DiscardingAsyncQueue()
        
        let wrapperTask = Task {
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            
            await #expect(throws: CancellationError.self) {
                try await queue.perform {
                    Issue.record("Operation should not run")
                }
            }
        }
        
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    @Test
    func cancelled_task_does_not_wait_for_previous_operations() async throws {
        let queue = DiscardingAsyncQueue()
        
        // GIVEN a first task that does not end until the test is complete.
        let (firstTaskStream, firstTaskContinuation) = AsyncStream.makeStream(of: Never.self)
        defer { firstTaskContinuation.finish() }
        queue.addTask {
            for await _ in firstTaskStream { }
        }
        
        // GIVEN a discardable task enqueued after the first task.
        let discardableTask = queue.addTask {
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
    func cancelled_operation_does_not_wait_for_previous_operations() async throws {
        let queue = DiscardingAsyncQueue()
        
        let (firstTaskStream, firstTaskContinuation) = AsyncStream.makeStream(of: Never.self)
        queue.addTask {
            for await _ in firstTaskStream { }
        }
        
        let wrapperTask = Task {
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            
            await #expect(throws: CancellationError.self) {
                try await queue.perform {
                    Issue.record("Operation should not run")
                }
            }
            
            firstTaskContinuation.finish()
            
            await #expect(throws: CancellationError.self) {
                try await queue.perform {
                    Issue.record("Operation should not run")
                }
            }
        }
        
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    // MARK: - Serialization precondition
    
    @Test
    func preconditionSerialized() async throws {
        let queue = DiscardingAsyncQueue()
        
        // Test all ways to run an operation:
        // - perform
        // - addTask
        
        try await queue.perform {
            queue.preconditionSerialized()
        }
        
        try await queue
            .addTask {
                queue.preconditionSerialized()
            }
            .value
    }
    
    @Test
    func preconditionSerialized_for_nested_queues() async throws {
        let queue1 = DiscardingAsyncQueue()
        let queue2 = DiscardingAsyncQueue()
        
        // Test all combinations:
        // - perform
        // - addTask
        
        try await queue2.perform {
            try await queue1.perform {
                queue2.preconditionSerialized()
                queue1.preconditionSerialized()
            }
            
            try await queue1
                .addTask {
                    queue2.preconditionSerialized()
                    queue1.preconditionSerialized()
                }
                .value
        }
        
        try await queue2
            .addTask {
                try await queue1.perform {
                    queue2.preconditionSerialized()
                    queue1.preconditionSerialized()
                }
                
                try await queue1
                    .addTask {
                        queue2.preconditionSerialized()
                        queue1.preconditionSerialized()
                    }
                    .value
            }
            .value
    }
    
    // MARK: - Isolation
    
    @Test
    func perform_closure_inherits_isolation() {
        // This test passes if it compiles without any error or warning.
        @MainActor class MyClass {
            let queue = DiscardingAsyncQueue()
            
            private func enqueue() async throws {
                try await queue.perform {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
        
        actor MyActor {
            let queue = DiscardingAsyncQueue()
            
            private func enqueue() async throws {
                try await queue.perform {
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
            let queue = DiscardingAsyncQueue()
            
            private func enqueue() -> Task<Void, any Error> {
                queue.addTask {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
        
        actor MyActor {
            let queue = DiscardingAsyncQueue()
            
            private func enqueue() -> Task<Void, any Error> {
                queue.addTask {
                    // Synchronous call is OK because the closure inherits
                    // the current isolation.
                    isolatedSynchronousMethod()
                }
            }
            
            private func isolatedSynchronousMethod() { }
        }
    }
}
