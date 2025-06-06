import AsyncQueues
import Testing

@Suite(.timeLimit(.minutes(1)))
struct AsyncQueueTests {
    @TaskLocal static var local: Int? = nil
    
    // MARK: - Current Task
    
    @Test
    func perform_executes_operation_in_the_current_task() async throws {
        let queue = AsyncQueue()
        
        let task = Task {
            await queue.perform {
                withUnsafeCurrentTask { currentTask in
                    currentTask?.cancel()
                }
                #expect(Task.isCancelled)
            }
            #expect(Task.isCancelled)
        }
        
        await task.value
    }
    
    @Test
    func perform_inherits_task_locals() async throws {
        let queue = AsyncQueue()
        
        let number = Int.random(in: 0..<1000)
        await Self.$local.withValue(number) {
            await queue.perform {
                #expect(Self.local == number)
            }
        }
    }
    
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
    func perform_returns_non_sendable_value() async throws {
        class NonSendable { }
        
        @globalActor actor MyGlobalActor {
            static let shared = MyGlobalActor()
        }
        
        let queue = AsyncQueue()
        
        do {
            let value = NonSendable()
            let result = await queue.perform {
                return value
            }
            #expect(result === value)
        }
        
        // Won't compile.
        //
        // do {
        //     let value = NonSendable()
        //     let result = await queue.perform { @MyGlobalActor in
        //         return value
        //     }
        //     #expect(result === value)
        // }
        
        do {
            let _ = await queue.perform {
                return NonSendable()
            }
        }
        
        do {
            let _ = await queue.perform { @MyGlobalActor in
                return NonSendable()
            }
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
    
    // MARK: - Ordering
    
    @Test
    func perform_is_ordered_with_nonthrowing_task() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = AsyncQueue()
        for i in 0...5000 {
            queue.addTask {
                valuesMutex.withLock { $0.append(i * 2) }
            }
            await queue.perform {
                valuesMutex.withLock { $0.append(i * 2 + 1) }
            }
        }
        let values = await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10001))
    }
    
    @Test
    func perform_is_ordered_with_throwing_task() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = AsyncQueue()
        func handleValue(_ value: Int) throws {
            valuesMutex.withLock { $0.append(value) }
        }
        for i in 0...5000 {
            queue.addTask {
                try handleValue(i * 2)
            }
            try await queue.perform {
                try handleValue(i * 2 + 1)
            }
        }
        let values = await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10001))
    }
    
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
    
    // MARK: - Cancellation
    
    @Test
    func nonthrowing_operation_is_cancelled_by_late_wrapper_task_cancellation() async throws {
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
    func throwing_operation_is_cancelled_by_late_wrapper_task_cancellation() async throws {
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
    func nonthrowing_operation_is_cancelled_by_early_wrapper_task_cancellation() async throws {
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
    func throwing_operation_is_cancelled_by_early_wrapper_task_cancellation() async throws {
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
    
    // MARK: - Serialization precondition
    
    @Test
    func preconditionSerialized() async throws {
        let queue = AsyncQueue()
        
        // Test all ways to run an operation:
        // - perform
        // - addTask (non-throwing)
        // - addTask (throwing)
        
        await queue.perform {
            queue.preconditionSerialized()
        }
        
        await queue
            .addTask {
                queue.preconditionSerialized()
            }
            .value
        
        try await queue
            .addTask {
                try Task.checkCancellation()
                queue.preconditionSerialized()
            }
            .value
    }
    
    @Test
    func preconditionSerialized_for_nested_queues() async throws {
        let queue1 = AsyncQueue()
        let queue2 = AsyncQueue()
        
        // Test all combinations:
        // - perform
        // - addTask (non-throwing)
        // - addTask (throwing)
        
        await queue2.perform {
            await queue1.perform {
                queue2.preconditionSerialized()
                queue1.preconditionSerialized()
            }
            
            await queue1
                .addTask {
                    queue2.preconditionSerialized()
                    queue1.preconditionSerialized()
                }
                .value
            
            try? await queue1
                .addTask {
                    try Task.checkCancellation()
                    queue2.preconditionSerialized()
                    queue1.preconditionSerialized()
                }
                .value
        }
        
        await queue2
            .addTask {
                await queue1.perform {
                    queue2.preconditionSerialized()
                    queue1.preconditionSerialized()
                }
                
                await queue1
                    .addTask {
                        queue2.preconditionSerialized()
                        queue1.preconditionSerialized()
                    }
                    .value
                
                try? await queue1
                    .addTask {
                        try Task.checkCancellation()
                        queue2.preconditionSerialized()
                        queue1.preconditionSerialized()
                    }
                    .value
            }
            .value
        
        try await queue2
            .addTask {
                await queue1.perform {
                    queue2.preconditionSerialized()
                    queue1.preconditionSerialized()
                }
                
                await queue1
                    .addTask {
                        queue2.preconditionSerialized()
                        queue1.preconditionSerialized()
                    }
                    .value
                
                try await queue1
                    .addTask {
                        try Task.checkCancellation()
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
}
