import Testing
@testable import Queue

@Suite(.timeLimit(.minutes(1)))
struct CoalescingAsyncQueueTests {
    @TaskLocal static var local: Int? = nil
    
    // MARK: - Current Task
    
    @Test
    func perform_executes_operation_in_the_current_task() async throws {
        // This test would faild with a policy.
        let queue = CoalescingAsyncQueue()
        
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
        let queue = CoalescingAsyncQueue()
        
        let number = Int.random(in: 0..<1000)
        try await Self.$local.withValue(number) {
            try await queue.perform {
                #expect(Self.local == number)
            }
        }
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func perform_inherits_task_locals(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        let number = Int.random(in: 0..<1000)
        try await Self.$local.withValue(number) {
            try await queue.perform(policy: policy) {
                #expect(Self.local == number)
            }
        }
    }
    
    // MARK: - Value and Error
    
    @Test
    func perform_returns_operation_value() async throws {
        let queue = CoalescingAsyncQueue()
        
        for value in 0..<10 {
            let result = try await queue.perform {
                return value
            }
            #expect(result == value)
        }
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func perform_returns_operation_value(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        for value in 0..<10 {
            let result = try await queue.perform(policy: policy) {
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
        
        let queue = CoalescingAsyncQueue()
        
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
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func perform_returns_non_sendable_value(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        class NonSendable { }
        
        @globalActor actor MyGlobalActor {
            static let shared = MyGlobalActor()
        }
        
        let queue = CoalescingAsyncQueue()
        
        // Won't compile.
        //
        // do {
        //     let value = NonSendable()
        //     let result = try await queue.perform(policy: policy) {
        //         return value
        //     }
        //     #expect(result === value)
        // }
        
        // Won't compile.
        //
        // do {
        //     let value = NonSendable()
        //     let result = try await queue.perform(policy: policy) { @MyGlobalActor in
        //         return value
        //     }
        //     #expect(result === value)
        // }
        
        do {
            let _ = try await queue.perform(policy: policy) {
                return NonSendable()
            }
        }
        
        // Won't compile.
        //
        // do {
        //     let _ = try await queue.perform(policy: policy) { @MyGlobalActor in
        //         return NonSendable()
        //     }
        // }
    }

    @Test
    func perform_rethrows_thrown_error() async throws {
        struct TestError: Error { }
        let queue = CoalescingAsyncQueue()
        
        await #expect(throws: TestError.self) {
            try await queue.perform {
                throw TestError()
            }
        }
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func perform_rethrows_thrown_error(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        struct TestError: Error { }
        let queue = CoalescingAsyncQueue()
        
        await #expect(throws: TestError.self) {
            try await queue.perform(policy: policy) {
                throw TestError()
            }
        }
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func added_task_returns_operation_value(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        for value in 0..<10 {
            let task = queue.addTask(policy: policy) {
                return value
            }
            let result = try await task.value
            #expect(result == value)
        }
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func added_task_rethrows_thrown_error(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        struct TestError: Error { }
        let queue = CoalescingAsyncQueue()
        
        let task = queue.addTask(policy: policy) {
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
        let queue = CoalescingAsyncQueue()
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
    func perform_is_ordered_with_task_using_policy_required() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = CoalescingAsyncQueue()
        for i in 0...5000 {
            queue.addTask(policy: .required) {
                valuesMutex.withLock { $0.append(i * 2) }
            }
            try await queue.perform(policy: .required) {
                valuesMutex.withLock { $0.append(i * 2 + 1) }
            }
        }
        let values = try await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values == Array(0...10001))
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func perform_is_ordered_with_task(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = CoalescingAsyncQueue()
        queue.addTask {
            valuesMutex.withLock { $0.append(1) }
        }
        try await queue.perform(policy: policy) {
            valuesMutex.withLock { $0.append(2) }
        }
        let values = valuesMutex.withLock { $0 }
        #expect(values == [1, 2])
    }
    
    @Test
    func tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = CoalescingAsyncQueue()
        for i in 0...10000 {
            let policy = [CoalescingAsyncQueue.Policy.required, .discardable].randomElement()!
            queue.addTask(policy: policy) {
                valuesMutex.withLock { $0.append(i) }
            }
        }
        let values = try await queue.perform {
            valuesMutex.withLock { $0 }
        }
        #expect(values.count > 1000)
        #expect(values == values.sorted())
    }
    
    @Test
    func cancelled_tasks_are_ordered() async throws {
        let valuesMutex = Mutex<[Int]>([])
        let queue = CoalescingAsyncQueue()
        for i in 0...10000 {
            let policy = [CoalescingAsyncQueue.Policy.required, .discardable].randomElement()!
            let task = queue.addTask(policy: policy) {
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
        let queue = CoalescingAsyncQueue()
        
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
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func operation_is_cancelled_by_late_wrapper_task_cancellation(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        // Semaphore-like stream: signal is `continuation.finish()`,
        // wait is `for await _ in stream { }`
        let (didStartStream, didStartContinuation) = AsyncStream.makeStream(of: Never.self)
        
        let wrapperTask = Task {
            try await queue.perform(policy: policy) {
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
        let queue = CoalescingAsyncQueue()
        
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
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func operation_is_cancelled_by_early_wrapper_task_cancellation(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        let wrapperTask = Task {
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            
            await #expect(throws: CancellationError.self) {
                try await queue.perform(policy: policy) {
                    Issue.record("Operation should not run")
                }
            }
        }
        
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func cancelled_task_does_not_wait_for_previous_operations(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        // GIVEN a first task that does not end until the test is complete.
        let (firstTaskStream, firstTaskContinuation) = AsyncStream.makeStream(of: Never.self)
        defer { firstTaskContinuation.finish() }
        queue.addTask {
            for await _ in firstTaskStream { }
        }
        
        // GIVEN a discardable task enqueued after the first task.
        let discardableTask = queue.addTask(policy: policy) {
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
        let queue = CoalescingAsyncQueue()
        
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
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func cancelled_operation_does_not_wait_for_previous_operations(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        let (firstTaskStream, firstTaskContinuation) = AsyncStream.makeStream(of: Never.self)
        queue.addTask {
            for await _ in firstTaskStream { }
        }
        
        let wrapperTask = Task {
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            
            await #expect(throws: CancellationError.self) {
                try await queue.perform(policy: policy) {
                    Issue.record("Operation should not run")
                }
            }
            
            firstTaskContinuation.finish()
            
            await #expect(throws: CancellationError.self) {
                try await queue.perform(policy: policy) {
                    Issue.record("Operation should not run")
                }
            }
        }
        
        wrapperTask.cancel()
        await wrapperTask.value
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func discardable_task_is_cancelled_by_early_subsequent_task(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        let task = queue.addTask(policy: .discardable) {
            Issue.record("Operation should not run")
        }
        queue.addTask(policy: policy) { }
        
        await #expect(throws: CancellationError.self) {
            try await task.value
        }
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func discardable_task_is_cancelled_by_late_subsequent_task(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        let (didStartStream, didStartContinuation) = AsyncStream.makeStream(of: Never.self)
        let task = queue.addTask(policy: .discardable) {
            didStartContinuation.finish()
            
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            #expect(Task.isCancelled)
        }
        
        for await _ in didStartStream { }
        queue.addTask(policy: policy) { }
        
        try await task.value
    }
    
    @Test
    func discardable_task_is_cancelled_by_late_subsequent_operation() async throws {
        let queue = CoalescingAsyncQueue()
        
        let (didStartStream, didStartContinuation) = AsyncStream.makeStream(of: Never.self)
        let task = queue.addTask(policy: .discardable) {
            didStartContinuation.finish()
            
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            #expect(Task.isCancelled)
        }
        
        for await _ in didStartStream { }
        try await queue.perform { }
        
        try await task.value
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func discardable_task_is_cancelled_by_late_subsequent_operation(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        let (didStartStream, didStartContinuation) = AsyncStream.makeStream(of: Never.self)
        let task = queue.addTask(policy: .discardable) {
            didStartContinuation.finish()
            
            // Wait until task is cancelled.
            let didCancelStream = AsyncStream<Never> { _ in }
            for await _ in didCancelStream { }
            #expect(Task.isCancelled)
        }
        
        for await _ in didStartStream { }
        try await queue.perform(policy: policy) { }
        
        try await task.value
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func discardable_operation_is_cancelled_by_late_subsequent_task(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        try await queue.perform(policy: .discardable) {
            queue.addTask(policy: policy) { }
            #expect(Task.isCancelled)
        }
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func required_task_is_not_cancelled_by_subsequent_task(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        let (didStartStream, didStartContinuation) = AsyncStream.makeStream(of: Never.self)
        let (secondTaskStream, secondTaskContinuation) = AsyncStream.makeStream(of: Never.self)
        queue.addTask(policy: .required) {
            didStartContinuation.finish()
            
            // Wait until other task is added
            for await _ in secondTaskStream { }
            #expect(!Task.isCancelled)
        }
        
        for await _ in didStartStream { }
        let secondTask = queue.addTask(policy: policy) { }
        secondTaskContinuation.finish()
        
        try await secondTask.value
    }
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func required_operation_is_not_cancelled_by_subsequent_task(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        try await queue.perform(policy: .required) {
            queue.addTask(policy: policy) { }
            #expect(!Task.isCancelled)
        }
    }
    
    // MARK: - Serialization precondition
    
    @Test
    func preconditionSerialized() async throws {
        let queue = CoalescingAsyncQueue()
        
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
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func preconditionSerialized(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue = CoalescingAsyncQueue()
        
        // Test all ways to run an operation:
        // - perform(policy:)
        // - addTask(policy:)
        
        try await queue.perform(policy: policy) {
            queue.preconditionSerialized()
        }
        
        try await queue
            .addTask(policy: policy) {
                queue.preconditionSerialized()
            }
            .value
    }
    
    @Test
    func preconditionSerialized_for_nested_queues() async throws {
        let queue1 = CoalescingAsyncQueue()
        let queue2 = CoalescingAsyncQueue()
        
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
    
    @Test(arguments: [CoalescingAsyncQueue.Policy.required, .discardable])
    func preconditionSerialized_for_nested_queues(
        policy: CoalescingAsyncQueue.Policy
    ) async throws {
        let queue1 = CoalescingAsyncQueue()
        let queue2 = CoalescingAsyncQueue()
        
        // Test all combinations:
        // - perform(policy:)
        // - addTask(policy:)
        
        try await queue2.perform(policy: policy) {
            try await queue1.perform(policy: policy) {
                queue2.preconditionSerialized()
                queue1.preconditionSerialized()
            }
            
            try await queue1
                .addTask(policy: policy) {
                    queue2.preconditionSerialized()
                    queue1.preconditionSerialized()
                }
                .value
        }
        
        try await queue2
            .addTask(policy: policy) {
                try await queue1.perform(policy: policy) {
                    queue2.preconditionSerialized()
                    queue1.preconditionSerialized()
                }
                
                try await queue1
                    .addTask(policy: policy) {
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
            let queue = CoalescingAsyncQueue()
            
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
            let queue = CoalescingAsyncQueue()
            
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
            let queue = CoalescingAsyncQueue()
            
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
            let queue = CoalescingAsyncQueue()
            
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
