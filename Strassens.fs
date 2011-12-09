module ParallelAlgorithms.Strassens

// Matrix multiplication with standard algorithm and Strassen's algorithm
// Translate from C# version at http://blogs.msdn.com/b/pfxteam/archive/2009/12/09/9934811.aspx#Strassens

// Lessons learned:
// 1. Pin arrays using GCHandle to avoid garbage collection's effect in parallelism.
// 2. Use indices to segment an array logically.
// 3. Strassen's algorithm is cache-oblivious, but it scales worse than the standard algorithm (need more optimizations).
// 4. Use 1-d arrays to represent matrix for better performance.
open System

open System.Collections.Concurrent
open System.Threading.Tasks

open System.Runtime.InteropServices

type Matrix(_matrix: int []) =

    let sqr x = x*x
    let isSquare n =
        sqr(int (sqrt(float n))) = n

    let size = if isSquare _matrix.Length then int (sqrt(float _matrix.Length)) else failwith "Unwellformed"
    let matrix = _matrix

    member x.Size
        with get() = size

    member x.Value
        with get() = matrix

// (n, m) = dimensions of A, B, and C submatrices 
// (ax,ay) = origin of A submatrix for multiplicand
// (bx,by) = origin of B submatrix for multiplicand  
// (cx,cy) = origin of C submatrix for result 
let inline private matrix_add(n, m, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS) =
    for i in 0..n-1 do
        for j in 0..m-1 do
            C.[(i+cx)*cS+j+cy] <- A.[(i+ax)*aS+j+ay] + B.[(i+bx)*bS+j+by]
                    
let inline private matrix_sub(n, m, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS) =
    for i in 0..n-1 do
        for j in 0..m-1 do
            C.[(i+cx)*cS+j+cy] <- A.[(i+ax)*aS+j+ay] - B.[(i+bx)*bS+j+by]
                    
let private matrix_mult_serial(l, m, n, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS) =
    for i in 0..l-1 do
        for j in 0..n-1 do
            let mutable tmp = 0
            for k in 0..m-1 do
                tmp <- tmp + A.[(i+ax)*aS+k+ay] * B.[(k+bx)*bS+j+by]
            C.[(i+cx)*cS+j+cy] <- tmp

let private matrix_mult_parallel(l, m, n, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS) =
    Parallel.For(0, l, 
        fun i -> for j in 0..n-1 do
                    let mutable tmp = 0
                    for k in 0..m-1 do
                        tmp <- tmp + A.[(i+ax)*aS+k+ay] * B.[(k+bx)*bS+j+by]
                    C.[(i+cx)*cS+j+cy] <- tmp) |> ignore

// s = Strassen's recursion limit for array dimensions 
let rec private strassen_mult_serial(n, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS, s) =
    if n <= s then
        matrix_mult_serial(
            n, n, n,
            A, ax, ay, aS,
            B, bx, by, bS,
            C, cx, cy, cS)
    else
        let n_2 = n >>> 1
                
        // Explicitly create buffer arrays.
        let buffer = Array.zeroCreate (9*n_2*n_2)
        let handle = GCHandle.Alloc(buffer, GCHandleType.Pinned)

        // p1 = (a11 + a22) * (b11 + b22) 
        matrix_add(n_2, n_2,
            A, ax, ay, aS,
            A, ax + n_2, ay + n_2, aS,
            buffer, 0, 0, n_2)
        matrix_add(n_2, n_2,
            B, bx, by, bS,
            B, bx + n_2, by + n_2, bS,
            buffer, n_2, 0, n_2)
        strassen_mult_serial(
            n_2,
            buffer,0,0, n_2,
            buffer, n_2, 0, n_2,
            buffer, 2*n_2, 0, n_2,
            s)
                
        // p2 = (a21 + a22) * b11 
        matrix_add(n_2, n_2,
            A, ax + n_2, ay, aS,
            A, ax + n_2, ay + n_2, aS,
            buffer, 0, 0, n_2)
        strassen_mult_serial(
            n_2,
            buffer, 0, 0, n_2,
            B, bx, by, bS,
            buffer, 3*n_2, 0, n_2,
            s)

        // p3 = a11 x (b12 - b22) 
        matrix_sub(n_2, n_2,
            B, bx, by + n_2, bS,
            B, bx + n_2, by + n_2, bS,
            buffer, n_2, 0, n_2)
        strassen_mult_serial(
            n_2,
            A, ax, ay, aS,
            buffer, n_2, 0, n_2,
            buffer, 4*n_2, 0, n_2,
            s)
                
        // p4 = a22 x (b21 - b11) 
        matrix_sub(n_2, n_2,
            B, bx + n_2, by, bS,
            B, bx, by, bS,
            buffer, n_2, 0, n_2)
        strassen_mult_serial(
            n_2,
            A, ax + n_2, ay + n_2, aS,
            buffer, n_2, 0, n_2,
            buffer, 5*n_2, 0, n_2,
            s)

        // p5 = (a11 + a12) x b22 
        matrix_add(n_2, n_2,
            A, ax, ay, aS,
            A, ax, ay + n_2, aS,
            buffer, 0, 0, n_2)
        strassen_mult_serial(
            n_2,
            buffer,0,0, n_2,
            B, bx + n_2, by + n_2, bS,
            buffer, 6*n_2, 0, n_2,
            s)
                
        // p6 = (a21 - a11) x (b11 + b12) 
        matrix_sub(n_2, n_2,
            A, ax + n_2, ay, aS,
            A, ax, ay, aS,
            buffer, 0, 0, n_2)
        matrix_add(n_2, n_2,
            B, bx, by, bS,
            B, bx, by + n_2, bS,
            buffer, n_2, 0, n_2)
        strassen_mult_serial(
            n_2,
            buffer, 0, 0, n_2,
            buffer, n_2, 0, n_2,
            buffer, 7*n_2, 0, n_2,
            s)

        // p7 = (a12 - a22) x (b21 + b22) 
        matrix_sub(n_2, n_2,
            A, ax, ay + n_2, aS,
            A, ax + n_2, ay + n_2, aS,
            buffer, 0, 0, n_2)
        matrix_add(n_2, n_2,
            B, bx + n_2, by, bS,
            B, bx + n_2, by + n_2, bS,
            buffer, n_2, 0, n_2)
        strassen_mult_serial(
            n_2,
            buffer, 0, 0, n_2,
            buffer, n_2, 0, n_2,
            buffer, 8*n_2, 0, n_2,
            s)

        // c11 = p1 + p4 - p5 + p7 
        matrix_add(n_2, n_2,
            buffer, 2*n_2, 0, n_2,
            buffer, 5*n_2, 0, n_2,
            C, cx, cy, cS)
        matrix_sub(n_2, n_2,
            C, cx, cy, cS,
            buffer, 6*n_2, 0, n_2,
            C, cx, cy, cS)
        matrix_add(n_2, n_2,
            C, cx, cy, cS,
            buffer, 8*n_2, 0, n_2,
            C, cx, cy, cS)

        // c12 = p3 + p5 
        matrix_add(n_2, n_2,
            buffer, 4*n_2, 0, n_2,
            buffer, 6*n_2, 0, n_2,
            C, cx, cy + n_2, cS);

        // c21 = p2 + p4 
        matrix_add(n_2, n_2,
            buffer, 3*n_2, 0, n_2,
            buffer, 5*n_2, 0, n_2,
            C, cx + n_2, cy, cS);

        // c22 = p1 + p3 - p2 + p6 
        matrix_add(n_2, n_2,
            buffer, 2*n_2, 0, n_2,
            buffer, 4*n_2, 0, n_2,
            C, cx + n_2, cy + n_2, cS)
        matrix_sub(n_2, n_2,
            C, cx + n_2, cy + n_2, cS,
            buffer, 3*n_2, 0, n_2,
            C, cx + n_2, cy + n_2, cS)
        matrix_add(n_2, n_2,
            C, cx + n_2, cy + n_2, cS,
            buffer, 7*n_2, 0, n_2,
            C, cx + n_2, cy + n_2, cS)
        handle.Free()

let rec private strassen_mult_parallel(n, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS, s) =
    if n <= s then
        matrix_mult_serial(
            n, n, n,
            A, ax, ay, aS,
            B, bx, by, bS,
            C, cx, cy, cS)
    else
        let n_2 = n >>> 1
                
        // Pin the buffer to delay garbage collection.
        let buffer = Array.zeroCreate (17*n_2*n_2)
        let handle = GCHandle.Alloc(buffer, GCHandleType.Pinned)

        // p1 = (a11 + a22) * (b11 + b22)
        let t_p1 = Task.Factory.StartNew( fun () ->  
            matrix_add(n_2, n_2,
                A, ax, ay, aS,
                A, ax + n_2, ay + n_2, aS,
                buffer, 0, 0, n_2)
            matrix_add(n_2, n_2,
                B, bx, by, bS,
                B, bx + n_2, by + n_2, bS,
                buffer, n_2, 0, n_2)
            strassen_mult_parallel(
                n_2,
                buffer, 0, 0, n_2,
                buffer, n_2, 0, n_2,
                buffer, 10*n_2, 0, n_2,
                s))
                
        // p2 = (a21 + a22) * b11 
        let t_p2 = Task.Factory.StartNew( fun () -> 
            matrix_add(n_2, n_2,
                A, ax + n_2, ay, aS,
                A, ax + n_2, ay + n_2, aS,
                buffer, 2*n_2, 0, n_2)
            strassen_mult_parallel(
                n_2,
                buffer, 2*n_2, 0, n_2,
                B, bx, by, bS,
                buffer, 11*n_2, 0, n_2,
                s))

        // p3 = a11 x (b12 - b22)
        let t_p3 = Task.Factory.StartNew( fun () ->  
            matrix_sub(n_2, n_2,
                B, bx, by + n_2, bS,
                B, bx + n_2, by + n_2, bS,
                buffer, 3*n_2, 0, n_2)
            strassen_mult_parallel(
                n_2,
                A, ax, ay, aS,
                buffer, 3*n_2, 0, n_2,
                buffer, 12*n_2, 0, n_2,
                s))
                
        // p4 = a22 x (b21 - b11) 
        let t_p4 = Task.Factory.StartNew( fun () -> 
            matrix_sub(n_2, n_2,
                B, bx + n_2, by, bS,
                B, bx, by, bS,
                buffer, 4*n_2, 0, n_2)
            strassen_mult_parallel(
                n_2,
                A, ax + n_2, ay + n_2, aS,
                buffer, 4*n_2, 0, n_2,
                buffer, 13*n_2, 0, n_2,
                s))

        // p5 = (a11 + a12) x b22 
        let t_p5 = Task.Factory.StartNew( fun () -> 
            matrix_add(n_2, n_2,
                A, ax, ay, aS,
                A, ax, ay + n_2, aS,
                buffer, 5*n_2, 0, n_2)
            strassen_mult_parallel(
                n_2,
                buffer, 5*n_2, 0, n_2,
                B, bx + n_2, by + n_2, bS,
                buffer, 14*n_2, 0, n_2,
                s))
                
        // p6 = (a21 - a11) x (b11 + b12) 
        let t_p6 = Task.Factory.StartNew( fun () -> 
            matrix_sub(n_2, n_2,
                A, ax + n_2, ay, aS,
                A, ax, ay, aS,
                buffer, 6*n_2, 0, n_2)
            matrix_add(n_2, n_2,
                B, bx, by, bS,
                B, bx, by + n_2, bS,
                buffer, 7*n_2, 0, n_2)
            strassen_mult_parallel(
                n_2,
                buffer, 6*n_2, 0, n_2,
                buffer, 7*n_2, 0, n_2,
                buffer, 15*n_2, 0, n_2,
                s))

        // p7 = (a12 - a22) x (b21 + b22) 
        let t_p7 = Task.Factory.StartNew( fun () -> 
            matrix_sub(n_2, n_2,
                A, ax, ay + n_2, aS,
                A, ax + n_2, ay + n_2, aS,
                buffer, 8*n_2, 0, n_2)
            matrix_add(n_2, n_2,
                B, bx + n_2, by, bS,
                B, bx + n_2, by + n_2, bS,
                buffer, 9*n_2, 0, n_2)
            strassen_mult_parallel(
                n_2,
                buffer, 8*n_2, 0, n_2,
                buffer, 9*n_2, 0, n_2,
                buffer, 16*n_2, 0, n_2,
                s))

        Task.WaitAll(t_p1, t_p2, t_p3, t_p4, t_p5, t_p6, t_p7)

        // c11 = p1 + p4 - p5 + p7 
        let t_c11 = Task.Factory.StartNew( fun () -> 
            matrix_add(n_2, n_2,
                buffer, 10*n_2, 0, n_2,
                buffer, 13*n_2, 0, n_2,
                buffer, 0, 0, n_2)
            matrix_sub(n_2, n_2,
                buffer, 0, 0, n_2,
                buffer, 14*n_2, 0, n_2,
                buffer, 0, 0, n_2)
            matrix_add(n_2, n_2,
                buffer, 0, 0, n_2,
                buffer, 16*n_2, 0, n_2,
                C, cx, cy, cS)) // Be careful with buffer's indices, easy to deadlock

        // c12 = p3 + p5 
        let t_c12 = Task.Factory.StartNew( fun () -> 
            matrix_add(n_2, n_2,
                buffer, 12*n_2, 0, n_2,
                buffer, 14*n_2, 0, n_2,
                C, cx, cy + n_2, cS))

        // c21 = p2 + p4 
        let t_c21 = Task.Factory.StartNew( fun () -> 
            matrix_add(n_2, n_2,
                buffer, 11*n_2, 0, n_2,
                buffer, 13*n_2, 0, n_2,
                C, cx + n_2, cy, cS))

        // c22 = p1 + p3 - p2 + p6 
        let t_c22 = Task.Factory.StartNew( fun () -> 
            matrix_add(n_2, n_2,
                buffer, 10*n_2, 0, n_2,
                buffer, 12*n_2, 0, n_2,
                buffer, n_2, 0, n_2)
            matrix_sub(n_2, n_2,
                buffer, n_2, 0, n_2,
                buffer, 11*n_2, 0, n_2,
                buffer, n_2, 0, n_2)
            matrix_add(n_2, n_2,
                buffer, n_2, 0, n_2,
                buffer, 15*n_2, 0, n_2,
                C, cx + n_2, cy + n_2, cS))

        Task.WaitAll(t_c11, t_c12, t_c21, t_c22)
        handle.Free()

let initEmptyMatrix (size) =
    Matrix(Array.zeroCreate (size*size))

let initRandomMatrix (size) =
    let rand = new Random()
    let matrix = Array.zeroCreate (size*size)
    for i in 0..size*size-1 do
            matrix.[i] <- rand.Next()%127
    Matrix(matrix)

let multiply(A: Matrix, B: Matrix) =
    if A.Size <> B.Size then failwith "Size mismatched"
    else
        let C = initEmptyMatrix(A.Size)
        let N = C.Size
        matrix_mult_serial(N, N, N, A.Value, 0, 0, N, B.Value, 0, 0, N, C.Value, 0, 0, N)
        C

let multiplyParallel(A: Matrix, B: Matrix) =
    if A.Size <> B.Size then failwith "Size mismatched"
    else
        let C = initEmptyMatrix(A.Size)
        let N = C.Size
        matrix_mult_parallel(N, N, N, A.Value, 0, 0, N, B.Value, 0, 0, N, C.Value, 0, 0, N)
        C

let multiplyStrassen(A: Matrix, B: Matrix) =
    if A.Size <> B.Size then failwith "Size mismatched"
    else
        let C = initEmptyMatrix(A.Size)
        let N = C.Size
        strassen_mult_serial(N, A.Value, 0, 0, N, B.Value, 0, 0, N, C.Value, 0, 0, N, 64)
        C

let multiplyStrassenParallel(A: Matrix, B: Matrix) =
    if A.Size <> B.Size then failwith "Size mismatched"
    else
        let C = initEmptyMatrix(A.Size)
        let N = C.Size
        strassen_mult_parallel(N, A.Value, 0, 0, N, B.Value, 0, 0, N, C.Value, 0, 0, N, 64)
        C