namespace ParallelAlgorithms

module Strassens =
    
    open System

    open System.Collections.Concurrent
    open System.Threading.Tasks

    type Matrix(initialSize: int) =
        let size = initialSize
        let matrix = Array.create (size*size) 0
                          
        // (n, m) = dimensions of A, B, and C submatrices 
        // (ax,ay) = origin of A submatrix for multiplicand
        // (bx,by) = origin of B submatrix for multiplicand  
        // (cx,cy) = origin of C submatrix for result 
        static member matrix_add(n, m, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS) =
            for i in 0..n-1 do
                for j in 0..m-1 do
                    C.[(i+cx)*cS+j+cy] <- A.[(i+ax)*aS+j+ay] + B.[(i+bx)*bS+j+by]
                    
        static member matrix_sub(n, m, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS) =
            for i in 0..n-1 do
                for j in 0..m-1 do
                    C.[(i+cx)*cS+j+cy] <- A.[(i+ax)*aS+j+ay] - B.[(i+bx)*bS+j+by]
                    
        static member matrix_mult_serial(l, m, n, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS) =
            for i in 0..l-1 do
                for j in 0..n-1 do
                    let mutable tmp = 0
                    for k in 0..m-1 do
                        tmp <- tmp + A.[(i+ax)*aS+k+ay] * B.[(k+bx)*bS+j+by]
                    C.[(i+cx)*cS+j+cy] <- tmp

        static member matrix_mult_parallel(l, m, n, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS) =
            Parallel.For(0, l, 
                fun i -> for j in 0..n-1 do
                            let mutable tmp = 0
                            for k in 0..m-1 do
                                tmp <- tmp + A.[(i+ax)*aS+k+ay] * B.[(k+bx)*bS+j+by]
                            C.[(i+cx)*cS+j+cy] <- tmp) |> ignore

        // s: Strassen's recursion limit for array dimensions 
        static member strassen_mult_serial(n, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS, s) =
            if n <= s then
                Matrix.matrix_mult_serial(
                    n, n, n,
                    A, ax, ay, aS,
                    B, bx, by, bS,
                    C, cx, cy, cS)
            else
                let n_2 = n >>> 1
                
                // Explicitly create buffer arrays
                let a_cum = Array.create (n_2*n_2) 0
                let b_cum = Array.create (n_2*n_2) 0
                let p1 = Array.create (n_2*n_2) 0
                let p2 = Array.create (n_2*n_2) 0
                let p3 = Array.create (n_2*n_2) 0
                let p4 = Array.create (n_2*n_2) 0
                let p5 = Array.create (n_2*n_2) 0
                let p6 = Array.create (n_2*n_2) 0
                let p7 = Array.create (n_2*n_2) 0

                 // p1 = (a11 + a22) * (b11 + b22) 
                Matrix.matrix_add(n_2, n_2,
                    A, ax, ay, aS,
                    A, ax + n_2, ay + n_2, aS,
                    a_cum, 0, 0, n_2)
                Matrix.matrix_add(n_2, n_2,
                    B, bx, by, bS,
                    B, bx + n_2, by + n_2, bS,
                    b_cum, 0, 0, n_2)
                Matrix.strassen_mult_serial(
                    n_2,
                    a_cum, 0, 0, n_2,
                    b_cum, 0, 0, n_2,
                    p1, 0, 0, n_2,
                    s)
                
                // p2 = (a21 + a22) * b11 
                Matrix.matrix_add(n_2, n_2,
                    A, ax + n_2, ay, aS,
                    A, ax + n_2, ay + n_2, aS,
                    a_cum, 0, 0, n_2)
                Matrix.strassen_mult_serial(
                    n_2,
                    a_cum, 0, 0, n_2,
                    B, bx, by, bS,
                    p2, 0, 0, n_2,
                    s)

                // p3 = a11 x (b12 - b22) 
                Matrix.matrix_sub(n_2, n_2,
                    B, bx, by + n_2, bS,
                    B, bx + n_2, by + n_2, bS,
                    b_cum, 0, 0, n_2)
                Matrix.strassen_mult_serial(
                    n_2,
                    A, ax, ay, aS,
                    b_cum, 0, 0, n_2,
                    p3, 0, 0, n_2,
                    s)
                
                // p4 = a22 x (b21 - b11) 
                Matrix.matrix_sub(n_2, n_2,
                    B, bx + n_2, by, bS,
                    B, bx, by, bS,
                    b_cum, 0, 0, n_2)
                Matrix.strassen_mult_serial(
                    n_2,
                    A, ax + n_2, ay + n_2, aS,
                    b_cum, 0, 0, n_2,
                    p4, 0, 0, n_2,
                    s)

                // p5 = (a11 + a12) x b22 
                Matrix.matrix_add(n_2, n_2,
                    A, ax, ay, aS,
                    A, ax, ay + n_2, aS,
                    a_cum, 0, 0, n_2)
                Matrix.strassen_mult_serial(
                    n_2,
                    a_cum, 0, 0, n_2,
                    B, bx + n_2, by + n_2, bS,
                    p5, 0, 0, n_2,
                    s)
                
                // p6 = (a21 - a11) x (b11 + b12) 
                Matrix.matrix_sub(n_2, n_2,
                    A, ax + n_2, ay, aS,
                    A, ax, ay, aS,
                    a_cum, 0, 0, n_2)
                Matrix.matrix_add(n_2, n_2,
                    B, bx, by, bS,
                    B, bx, by + n_2, bS,
                    b_cum, 0, 0, n_2)
                Matrix.strassen_mult_serial(
                    n_2,
                    a_cum, 0, 0, n_2,
                    b_cum, 0, 0, n_2,
                    p6, 0, 0, n_2,
                    s)

                // p7 = (a12 - a22) x (b21 + b22) 
                Matrix.matrix_sub(n_2, n_2,
                    A, ax, ay + n_2, aS,
                    A, ax + n_2, ay + n_2, aS,
                    a_cum, 0, 0, n_2)
                Matrix.matrix_add(n_2, n_2,
                    B, bx + n_2, by, bS,
                    B, bx + n_2, by + n_2, bS,
                    b_cum, 0, 0, n_2)
                Matrix.strassen_mult_serial(
                    n_2,
                    a_cum, 0, 0, n_2,
                    b_cum, 0, 0, n_2,
                    p7, 0, 0, n_2,
                    s)

                // c11 = p1 + p4 - p5 + p7 
                Matrix.matrix_add(n_2, n_2,
                    p1, 0, 0, n_2,
                    p4, 0, 0, n_2,
                    C, cx, cy, cS)
                Matrix.matrix_sub(n_2, n_2,
                    C, cx, cy, cS,
                    p5, 0, 0, n_2,
                    C, cx, cy, cS)
                Matrix.matrix_add(n_2, n_2,
                    C, cx, cy, cS,
                    p7, 0, 0, n_2,
                    C, cx, cy, cS)

                // c12 = p3 + p5 
                Matrix.matrix_add(n_2, n_2,
                    p3, 0, 0, n_2,
                    p5, 0, 0, n_2,
                    C, cx, cy + n_2, cS);

                // c21 = p2 + p4 
                Matrix.matrix_add(n_2, n_2,
                    p2, 0, 0, n_2,
                    p4, 0, 0, n_2,
                    C, cx + n_2, cy, cS);

                // c22 = p1 + p3 - p2 + p6 
                Matrix.matrix_add(n_2, n_2,
                    p1, 0, 0, n_2,
                    p3, 0, 0, n_2,
                    C, cx + n_2, cy + n_2, cS)
                Matrix.matrix_sub(n_2, n_2,
                    C, cx + n_2, cy + n_2, cS,
                    p2, 0, 0, n_2,
                    C, cx + n_2, cy + n_2, cS)
                Matrix.matrix_add(n_2, n_2,
                    C, cx + n_2, cy + n_2, cS,
                    p6, 0, 0, n_2,
                    C, cx + n_2, cy + n_2, cS)

        static member strassen_mult_parallel(n, A: _ [], ax, ay, aS, B: _ [], bx, by, bS, C: _ [], cx, cy, cS, s) =
            if n <= s then
                Matrix.matrix_mult_serial(
                    n, n, n,
                    A, ax, ay, aS,
                    B, bx, by, bS,
                    C, cx, cy, cS)
            else
                let n_2 = n >>> 1
                
                // Explicitly create buffer arrays
                
                let p1 = Array.create (n_2*n_2) 0
                let p2 = Array.create (n_2*n_2) 0
                let p3 = Array.create (n_2*n_2) 0
                let p4 = Array.create (n_2*n_2) 0
                let p5 = Array.create (n_2*n_2) 0
                let p6 = Array.create (n_2*n_2) 0
                let p7 = Array.create (n_2*n_2) 0

                // p1 = (a11 + a22) * (b11 + b22)
                let t_p1 = Task.Factory.StartNew( fun () -> 
                    let a_cum = Array.create (n_2*n_2) 0
                    let b_cum = Array.create (n_2*n_2) 0 
                    Matrix.matrix_add(n_2, n_2,
                        A, ax, ay, aS,
                        A, ax + n_2, ay + n_2, aS,
                        a_cum, 0, 0, n_2)
                    Matrix.matrix_add(n_2, n_2,
                        B, bx, by, bS,
                        B, bx + n_2, by + n_2, bS,
                        b_cum, 0, 0, n_2)
                    Matrix.strassen_mult_serial(
                        n_2,
                        a_cum, 0, 0, n_2,
                        b_cum, 0, 0, n_2,
                        p1, 0, 0, n_2,
                        s))
                
                // p2 = (a21 + a22) * b11 
                let t_p2 = Task.Factory.StartNew( fun () -> 
                    let c_cum = Array.create (n_2*n_2) 0
                    Matrix.matrix_add(n_2, n_2,
                        A, ax + n_2, ay, aS,
                        A, ax + n_2, ay + n_2, aS,
                        c_cum, 0, 0, n_2)
                    Matrix.strassen_mult_serial(
                        n_2,
                        c_cum, 0, 0, n_2,
                        B, bx, by, bS,
                        p2, 0, 0, n_2,
                        s))

                // p3 = a11 x (b12 - b22)
                let t_p3 = Task.Factory.StartNew( fun () ->  
                    let d_cum = Array.create (n_2*n_2) 0
                    Matrix.matrix_sub(n_2, n_2,
                        B, bx, by + n_2, bS,
                        B, bx + n_2, by + n_2, bS,
                        d_cum, 0, 0, n_2)
                    Matrix.strassen_mult_serial(
                        n_2,
                        A, ax, ay, aS,
                        d_cum, 0, 0, n_2,
                        p3, 0, 0, n_2,
                        s))
                
                // p4 = a22 x (b21 - b11) 
                let t_p4 = Task.Factory.StartNew( fun () -> 
                    let e_cum = Array.create (n_2*n_2) 0
                    Matrix.matrix_sub(n_2, n_2,
                        B, bx + n_2, by, bS,
                        B, bx, by, bS,
                        e_cum, 0, 0, n_2)
                    Matrix.strassen_mult_serial(
                        n_2,
                        A, ax + n_2, ay + n_2, aS,
                        e_cum, 0, 0, n_2,
                        p4, 0, 0, n_2,
                        s))

                // p5 = (a11 + a12) x b22 
                let t_p5 = Task.Factory.StartNew( fun () -> 
                    let f_cum = Array.create (n_2*n_2) 0

                    Matrix.matrix_add(n_2, n_2,
                        A, ax, ay, aS,
                        A, ax, ay + n_2, aS,
                        f_cum, 0, 0, n_2)
                    Matrix.strassen_mult_serial(
                        n_2,
                        f_cum, 0, 0, n_2,
                        B, bx + n_2, by + n_2, bS,
                        p5, 0, 0, n_2,
                        s))
                
                // p6 = (a21 - a11) x (b11 + b12) 
                let t_p6 = Task.Factory.StartNew( fun () ->
                    let g_cum = Array.create (n_2*n_2) 0
                    let h_cum = Array.create (n_2*n_2) 0 
                    Matrix.matrix_sub(n_2, n_2,
                        A, ax + n_2, ay, aS,
                        A, ax, ay, aS,
                        g_cum, 0, 0, n_2)
                    Matrix.matrix_add(n_2, n_2,
                        B, bx, by, bS,
                        B, bx, by + n_2, bS,
                        h_cum, 0, 0, n_2)
                    Matrix.strassen_mult_serial(
                        n_2,
                        g_cum, 0, 0, n_2,
                        h_cum, 0, 0, n_2,
                        p6, 0, 0, n_2,
                        s))

                // p7 = (a12 - a22) x (b21 + b22) 
                let t_p7 = Task.Factory.StartNew( fun () -> 
                    let i_cum = Array.create (n_2*n_2) 0
                    let j_cum = Array.create (n_2*n_2) 0
                    Matrix.matrix_sub(n_2, n_2,
                        A, ax, ay + n_2, aS,
                        A, ax + n_2, ay + n_2, aS,
                        i_cum, 0, 0, n_2)
                    Matrix.matrix_add(n_2, n_2,
                        B, bx + n_2, by, bS,
                        B, bx + n_2, by + n_2, bS,
                        j_cum, 0, 0, n_2)
                    Matrix.strassen_mult_serial(
                        n_2,
                        i_cum, 0, 0, n_2,
                        j_cum, 0, 0, n_2,
                        p7, 0, 0, n_2,
                        s))

                Task.WaitAll(t_p1, t_p2, t_p3, t_p4, t_p5, t_p6, t_p7)

                // c11 = p1 + p4 - p5 + p7 
                let t_c11 = Task.Factory.StartNew( fun () -> 
                    Matrix.matrix_add(n_2, n_2,
                        p1, 0, 0, n_2,
                        p4, 0, 0, n_2,
                        C, cx, cy, cS)
                    Matrix.matrix_sub(n_2, n_2,
                        C, cx, cy, cS,
                        p5, 0, 0, n_2,
                        C, cx, cy, cS)
                    Matrix.matrix_add(n_2, n_2,
                        C, cx, cy, cS,
                        p7, 0, 0, n_2,
                        C, cx, cy, cS))

                // c12 = p3 + p5 
                let t_c12 = Task.Factory.StartNew( fun () -> 
                    Matrix.matrix_add(n_2, n_2,
                        p3, 0, 0, n_2,
                        p5, 0, 0, n_2,
                        C, cx, cy + n_2, cS))

                // c21 = p2 + p4 
                let t_c21 = Task.Factory.StartNew( fun () -> 
                    Matrix.matrix_add(n_2, n_2,
                        p2, 0, 0, n_2,
                        p4, 0, 0, n_2,
                        C, cx + n_2, cy, cS))

                // c22 = p1 + p3 - p2 + p6 
                let t_c22 = Task.Factory.StartNew( fun () -> 
                    Matrix.matrix_add(n_2, n_2,
                        p1, 0, 0, n_2,
                        p3, 0, 0, n_2,
                        C, cx + n_2, cy + n_2, cS)
                    Matrix.matrix_sub(n_2, n_2,
                        C, cx + n_2, cy + n_2, cS,
                        p2, 0, 0, n_2,
                        C, cx + n_2, cy + n_2, cS)
                    Matrix.matrix_add(n_2, n_2,
                        C, cx + n_2, cy + n_2, cS,
                        p6, 0, 0, n_2,
                        C, cx + n_2, cy + n_2, cS))

                Task.WaitAll(t_c11, t_c12, t_c21, t_c22)

        static member multiply(A: Matrix, B: Matrix) =
            if A.Size <> B.Size then failwith "Size mismatched"
            else
                let C = Matrix(A.Size)
                let N = C.Size
                Matrix.matrix_mult_serial(N, N, N, A.Value, 0, 0, N, B.Value, 0, 0, N, C.Value, 0, 0, N)
                C

        static member multiplyParallel(A: Matrix, B: Matrix) =
            if A.Size <> B.Size then failwith "Size mismatched"
            else
                let C = Matrix(A.Size)
                let N = C.Size
                Matrix.matrix_mult_parallel(N, N, N, A.Value, 0, 0, N, B.Value, 0, 0, N, C.Value, 0, 0, N)
                C

        static member multiplyStrassen(A: Matrix, B: Matrix) =
            if A.Size <> B.Size then failwith "Size mismatched"
            else
                let C = Matrix(A.Size)
                let N = C.Size
                Matrix.strassen_mult_serial(N, A.Value, 0, 0, N, B.Value, 0, 0, N, C.Value, 0, 0, N, 64)
                C

        static member multiplyStrassenParallel(A: Matrix, B: Matrix) =
            if A.Size <> B.Size then failwith "Size mismatched"
            else
                let C = Matrix(A.Size)
                let N = C.Size
                Matrix.strassen_mult_parallel(N, A.Value, 0, 0, N, B.Value, 0, 0, N, C.Value, 0, 0, N, 64)
                C

        member C.fillRandom () =
            let rand = new Random()
            for i in 0..size*size-1 do
                    matrix.[i] <- rand.Next()%15

        member x.Size
            with get() = size

        member x.Value
            with get() = matrix





