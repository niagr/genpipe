/**
 * Encapsulates an iterable and allows chaining of transformations.
 */
export class Pipeline<T> {
    private readonly iter: Generator<T>

    constructor(iter: Generator<T>) {
        this.iter = iter
    }

    /**
     * Transform the pipeline using the given function, and returns a new pipeline.
     * The function should accept an iterable and should return a generator.
     * The pipeline's backing generator will be passed to the function, and the returned generator
     * will be used to create the new pipeline that is returned.
     *
     * Example:
     * ```
     * const x = new Pipeline(range(0, 40))
     *    .transform(F.filter((i) => i % 3 == 0))
     *    .transform(F.map((i) => i * 2))
     * ```
     */
    transform<U>(func: (iter: Iterable<T>) => Generator<U>): Pipeline<U> {
        return new Pipeline(func(this.iter))
    }

    /**
     * Alias for `transform`.
     */
    tf = this.transform

    /**
     * Convenience method to convert the iterable to an array.
     */
    eval(): T[] {
        return [...this.iter]
    }

    /**
     * Convenience method to convert the pipeline to an async pipeline.
     */
    toAsync(): AsyncPipeline<T> {
        return AsyncPipeline.fromSync(this.iter)
    }

    /**
     * Convenience method to convert the pipeline to a generator.
     */
    toGen(): Generator<T> {
        return this.iter
    }
}

export class AsyncPipeline<T> {
    private readonly iter: AsyncGenerator<T>

    constructor(iter: AsyncIterable<T>) {
        this.iter = toAsyncGen(iter)
    }

    /**
     * Transform the pipeline using the given function, and returns a new pipeline.
     * The function should accept an async iterable and should return an async generator.
     * The pipeline's backing async generator will be passed to the function, and the returned
     * async generator will be used to create the new pipeline that is returned.
     *
     * Example:
     * ```
     * const x = new AsyncPipeline(range(0, 40))
     *   .transform(F.filterAsync((i) => i % 3 == 0))
     *   .transform(F.mapAsync((i) => i * 2))
     * ```
     */
    transform<U>(func: (iter: AsyncIterable<T>) => AsyncGenerator<U>): AsyncPipeline<U> {
        return new AsyncPipeline(func(this.iter))
    }

    /**
     * Alias for `transform`.
     */
    tf = this.transform

    /**
     * Convenience method to convert the async pipeline to an array.
     */
    async eval(): Promise<T[]> {
        const results: T[] = []
        for await (const x of this.iter) {
            results.push(x)
        }
        return results
    }

    /**
     * Convenience method to create an async pipeline from a sync iterable.
     */
    static fromSync<T>(iter: Iterable<T>): AsyncPipeline<T> {
        return new AsyncPipeline(toAsyncGen(iter))
    }

    /**
     * Convenience method to convert the async pipeline to an async generator.
     */
    toAsyncGen(): AsyncGenerator<T> {
        return toAsyncGen(this.iter)
    }
}

/**
 * Convenience function to convert an iterable to a generator.
 */
function* toGen<T>(iter: Iterable<T>): Generator<T> {
    for (const x of iter) {
        yield x
    }
}

/**
 * Convenience function to convert an iterable or async iterable to an async generator.
 */
async function* toAsyncGen<T>(iter: Iterable<T> | AsyncIterable<T>): AsyncGenerator<T> {
    if (Symbol.iterator in iter) {
        yield* toGen(iter)
    } else if (Symbol.asyncIterator in iter) {
        for await (const x of iter) {
            yield x
        }
    } else {
        throw new Error(`Not an iterable: ${iter}`)
    }
}

/**
 * Generator that yields the numbers from start to stop (exclusive)
 */
export function* range(start: number, stop: number): Generator<number> {
    for (let i = start; i < stop; i++) {
        yield i
    }
}

/**
 * Generator that yields the elements of the given iterable in batches of the given size.
 * No batches are yielded for empty iterables, and zero or negative batch sizes.
 */
export function* batch<T>(iter: Iterable<T>, batchSize: number): Generator<T[]> {
    const iterator = iter[Symbol.iterator]()
    while (true) {
        const nextBatch: T[] = []
        while (nextBatch.length < batchSize) {
            const res = iterator.next()
            if (res.done) {
                break
            }
            nextBatch.push(res.value)
        }
        if (nextBatch.length == 0) {
            break
        }
        yield nextBatch
    }
}

/**
 * `Array.map` for generators.
 *
 * Generator that applies the given function to each element of the given iterable
 * and yields the result.
 */
export function* map<T, U>(iter: Iterable<T>, func: (t: T) => U): Generator<U> {
    for (const x of iter) {
        yield func(x)
    }
}

/**
 * `Array.map` for async generators.
 *
 * Generator that applies the given function to each element of the given iterable,
 * and awaits the result if it is a promise, and yields the result.
 */
export async function* mapAsync<T, U>(
    iter: AsyncIterable<T>,
    func: (t: T) => Promise<U> | U,
): AsyncGenerator<U> {
    for await (const x of iter) {
        yield await func(x)
    }
}

/**
 * `Array.forEach` for generators.
 *
 * Generator that applies the given function to each element of the given iterable
 * and yields the element.
 */
export function* forEach<T>(iter: Iterable<T>, func: (t: T) => void): Generator<T> {
    for (const x of iter) {
        func(x)
        yield x
    }
}

/**
 * `Array.forEach` for async generators.
 *
 * Generator that applies the given function to each element of the given iterable,
 * and awaits the result if it is a promise, and yields the element.
 */
export async function* forEachAsync<T>(
    iter: AsyncIterable<T>,
    func: (t: T) => Promise<void> | void,
): AsyncGenerator<T> {
    for await (const x of iter) {
        await func(x)
        yield x
    }
}

/**
 * `Array.filter` for generators.
 *
 * Generator that yields the elements of the given iterable that satisfy the given predicate.
 */
export function* filter<T>(iter: Iterable<T>, func: (t: T) => boolean): Generator<T> {
    for (const x of iter) {
        if (func(x)) {
            yield x
        }
    }
}

/**
 * `Array.filter` for async generators.
 *
 * Generator that yields the elements of the given iterable that satisfy the given predicate.
 * The predicate can return a promise, in which case it will be awaited.
 */
export async function* filterAsync<T>(
    iter: AsyncIterable<T>,
    func: (t: T) => Promise<boolean> | boolean,
): AsyncGenerator<T> {
    for await (const x of iter) {
        if (await func(x)) {
            yield x
        }
    }
}

/**
 * `Array.reduce` for generators.
 *
 * Generator that applies the given reducer function to each element of the given iterable
 * and yields the result.
 */
export function reduce<T, A>(iter: Iterable<T>, func: (acc: A, t: T) => A, initialValue: A): A {
    let acc = initialValue
    for (const x of iter) {
        acc = func(acc, x)
    }
    return acc
}

/**
 * Asynchronously execute an iterable of tasks with a given concurrency limit, yielding results as
 * they complete.
 *
 * The tasks are functions that start an asynchronous operation and return a promise. Tasks will be
 * started up to the concurrency limit, and as soon as one completes, the result is yielded and
 * another task is started.
 *
 * If any of the tasks throw an error, the generator will throw that error immediately without
 * waiting for the other tasks.
 */
export async function* execConcurrently<R>(
    tasks: AsyncIterable<() => Promise<R>> | Iterable<() => Promise<R>>,
    limit: number,
): AsyncGenerator<R> {
    const executing = new Set<Promise<R>>()

    function scheduleTask(task: () => Promise<R>) {
        const promise = task().finally(() => {
            executing.delete(promise)
        })
        executing.add(promise)
    }

    if (Symbol.iterator in tasks) {
        tasks = toAsyncGen(tasks)
    }
    const tasksIter = tasks[Symbol.asyncIterator]()

    while (true) {
        const nextResult = await tasksIter.next()
        if (!nextResult.done) {
            while (executing.size >= limit) {
                yield await Promise.race(executing)
            }
            scheduleTask(nextResult.value)
        } else if (executing.size > 0) {
            yield await Promise.race(executing)
        } else {
            break
        }
    }
}

/**
 * Generator that applies the given async function to each element of the given iterable
 * concurrently.
 */
export function mapConcurrent<T, U>(
    iter: AsyncIterable<T>,
    concurrencyLimit: number,
    func: (t: T) => Promise<U>,
): AsyncGenerator<U> {
    return new AsyncPipeline(iter)
        .tf(F.mapAsync((x) => () => func(x)))
        .tf(F.concurrent(concurrencyLimit))
        .toAsyncGen()
}

/**
 * A queue that yields its items in consecutive order.
 *
 * Items are added to the queue with an index. The queue will yield items only in consecutive order
 * of their index. If the item at the next index is not present, the queue will throw an error.
 *
 * This is useful for maintaining the order of items that are processed concurrently.
 */
class ConsecutiveQueue<T> {
    private readonly idxToItem: Map<number, T>
    private currIdx = 0

    constructor() {
        this.idxToItem = new Map()
    }

    add(idx: number, item: T) {
        this.idxToItem.set(idx, item)
    }

    hasNext(): boolean {
        return this.idxToItem.has(this.currIdx)
    }

    remove(): T {
        if (!this.hasNext()) {
            throw new Error(`No item at current index ${this.currIdx}`)
        }
        const item = this.idxToItem.get(this.currIdx)!
        this.idxToItem.delete(this.currIdx)
        this.currIdx++
        return item
    }

    *removeOrdered(): Generator<T> {
        while (this.hasNext()) {
            yield this.remove()
        }
    }
}

/**
 * Asynchronously execute an iterable of tasks with a given concurrency limit, yielding results
 * in the same order as the tasks.
 */
export async function* concurrentInOrder<R>(
    tasks: AsyncIterable<() => Promise<R>> | Iterable<() => Promise<R>>,
    limit: number,
): AsyncGenerator<R> {
    const executing = new Set<Promise<R>>()
    const results = new ConsecutiveQueue<R>()

    function scheduleTask(idx: number, task: () => Promise<R>) {
        const promise = task()
            .then((result) => {
                results.add(idx, result)
                return result
            })
            .finally(() => {
                executing.delete(promise)
            })
        executing.add(promise)
    }

    if (Symbol.iterator in tasks) {
        tasks = toAsyncGen(tasks)
    }
    const tasksIter = tasks[Symbol.asyncIterator]()
    let currIdx = 0

    while (true) {
        const nextResult = await tasksIter.next()
        if (!nextResult.done) {
            while (executing.size >= limit) {
                await Promise.race(executing)
                yield* results.removeOrdered()
            }
            scheduleTask(currIdx++, nextResult.value)
        } else if (executing.size > 0) {
            await Promise.race(executing)
            yield* results.removeOrdered()
        } else {
            break
        }
    }
}

/**
 * Utility function to partially apply a function's arguments except the first one.
 */
function partial<T, A1, R>(func: (t: T, a1: A1) => R): (a1: A1) => (t: T) => R
function partial<T, A1, A2, R>(func: (t: T, a1: A1, a2: A2) => R): (a1: A1, a2: A2) => (t: T) => R
function partial<T, A1, A2, A3, R>(
    func: (t: T, a1: A1, a2: A2, a3: A3) => R,
): (a1: A1, a2: A2, a3: A3) => (t: T) => R
function partial<T, A, R>(func: (t: T, ...a: A[]) => R): (...a: A[]) => (t: T) => R {
    return (...a: A[]) => (t) => func(t, ...a)
}

async function delay(ms: number): Promise<void> {
    return await new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Partially applied versions of the functions in this module.
 * Useful for chaining transformations.
 */
export const F = {
    batch: partial(batch),
    concurrent: partial(execConcurrently),
    concurrentInOrder: partial(concurrentInOrder),
    filter: partial(filter),
    filterAsync: partial(filterAsync),
    forEach: partial(forEach),
    forEachAsync: partial(forEachAsync),
    map: partial(map),
    mapAsync: partial(mapAsync),
    mapConcurrent: partial(mapConcurrent),
    reduce: partial(reduce),
} as const

export const H = {
    logEach: F.forEach(console.log),
    logEachAsync: F.forEachAsync(console.log),
} as const

// if (import.meta.main) {
//     const x = new Pipeline(toGen([20, 5, 2, 1]))
//         // .tf(F.filter((i) => i % 3 == 0))
//         // .tf(F.map((i) => i * 2))

//         // .toAsync()
//         // .tf(F.mapConcurrent(10, async (i) => {
//         //     const timeout = 1000 * Math.max(0.5, Math.random())
//         //     console.log(`starting timeout async function ${i}`)
//         //     await delay(timeout)
//         //     console.log(`finished timeout async function ${i}`)
//         //     return i + 2
//         // }))
//         // .eval()

//         .tf(F.map((i) => async () => {
//             const timeout = 1000 * i
//             console.log(`starting timeout async function ${i}`)
//             await delay(timeout)
//             console.log(`finished timeout async function ${i}`)
//             return i
//         }))
//         .toAsync()
//         .tf(F.concurrentInOrder(10))
//         .eval()

//     // .tf(F.map((i) => ({
//     //     orig: i,
//     //     double: i * 2,
//     // })))
//     // .eval()

//     console.log(await x)
// }
