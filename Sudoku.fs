module ParallelAlgorithms.Sudoku

// A failed attempt to parallelize a sudoku solver.

// Lessons learned:
// 1. Depth-first search is difficult to parallelize (they are soon to be completed).
// 2. It's hard to ensure correctness when doing mutation inside the grid.
// 3. The waitAny implementation is slow, due to large overheads.

open System

open System.Collections.Concurrent
open System.Threading.Tasks
open System.Threading

// Limitation: may terminate when the result is None
let waitAny (f: _ -> 'T, dataset) = 
    let cts = new CancellationTokenSource()
    let token = cts.Token
    let tasks = 
        [| for d in dataset ->
                Task.Factory.StartNew((fun () -> f(d)), token) |]
    let id = Task.WaitAny([| for t in tasks -> t :> Task|])
    printfn "Task %i finished" (id+1)
    cts.Cancel()
    try
        try Task.WaitAll([| for t in tasks -> t :> Task|]) 
        with 
        | :? AggregateException as ae ->
                ae.Flatten().Handle(fun e -> e :? OperationCanceledException)
    finally
        if cts <> null then cts.Dispose()
    tasks.[id].Result

type Board = int [][]

type Solver(initialSize: int) =

    let size = initialSize
         
    member private x.containsRow(n, r, grid: Board) =
        let rec loop i =
            if i = grid.Length then false
            elif grid.[r].[i] = n then true
            else loop (i+1)
        loop 0

    member private x.containsColumn(n, c, grid: Board) =
        let rec loop i =
            if i = grid.Length then false
            elif grid.[i].[c] = n then true
            else loop (i+1)
        loop 0

    member private x.containsSquare(n, r, c, grid: Board) =
        let r0 = r-r%size
        let c0 = c-c%size
        let rec loop(i,j) =
            if i = size then false
            elif j = size then loop(i+1, 0)
            elif n = grid.[r0+i].[c0+j] then true
            else loop (i, j+1)
        loop(0, 0)

    member private x.safe(n, r, c, grid: Board) =
        not(x.containsRow(n, r, grid) || x.containsColumn(n, c, grid) || x.containsSquare(n, r, c, grid))

    // Caution: this function has side effect
    member private x.placeNumber(n, r, c, grid: Board) =
        grid.[r].[c] <- n

    // Caution: this function has side effect
    member private x.removeNumber(r, c, grid: Board) =
        grid.[r].[c] <- 0

    member x.findPosition(grid: Board) =
        let rec loop(i,j) =
            if i = grid.Length then None
            elif j = grid.Length then loop(i+1, 0)
            elif grid.[i].[j] = 0 then Some (i, j)
            else loop (i, j+1)
        loop(0, 0)

    member private x.recover(grid: Board, acc, acc') =
        match acc' with
        | _ when acc' = acc -> ()
        | (r,c)::acc'' -> x.removeNumber(r, c, grid)
                          x.recover(grid, acc, acc'')
        | _   -> ()

    // This is the most trivial way to iterate through the whole range of candidates
    member x.solve(sud: Board) =
        let grid: Board = Array.init sud.Length (fun i -> Array.copy sud.[i]) // copying for mutating grid later
        let rec sudSolve(i, acc) =
            match x.findPosition grid with
            | Some (r, c) -> //printfn "%i %i %i" r c i
                                if i > grid.Length then None, acc
                                elif x.safe(i, r, c, grid) then
                                    x.placeNumber(i, r, c, grid)
                                    match sudSolve(1, (r, c)::acc) with
                                    | None, acc' -> x.recover(grid, acc, acc')
                                                    sudSolve(i+1, acc)
                                    | g          -> g
                                else
                                    sudSolve (i+1, acc)
            | None        -> Some grid, acc
        fst(sudSolve(1, []))

    member private x.generateBoards (grid: Board) =
        let boards = new ResizeArray<Board>(grid.Length)
        let rec loop level =
            match x.findPosition(grid) with
            | Some (r, c) ->  if level > 0 then
                                    for i in 1..grid.Length do
                                    if x.safe(i, r, c, grid) then
                                        x.placeNumber(i, r, c, grid)
                                        loop (level-1)
                                        x.removeNumber(r, c, grid)
                                else
                                    for i in 1..grid.Length do
                                    if x.safe(i, r, c, grid) then
                                        x.placeNumber(i, r, c, grid)
                                        boards.Add(Array.copy grid)
                                        x.removeNumber(r, c, grid)
            | None  -> ()
        loop 3
        boards.ToArray()

    member x.solveParallel(sud: Board) =
        let grid: Board = Array.init sud.Length (fun i -> Array.copy sud.[i])
        let boards = x.generateBoards grid
        printfn "Generated %i boards" boards.Length
        // Faster than waitAny function but does not terminate other threads properly.
        let result = ref None
        let monitor = new Object()
        Parallel.For(0, boards.Length, 
            fun i (loopState: ParallelLoopState) ->
                    let r = x.solve (boards.[i])
                    if r <> None then                            
                        lock monitor (fun () -> result := r)
                        printfn "Task %i finished" i
                        loopState.Stop()) |> ignore
        !result
        //waitAny(x.solve, boards)
                
let solve(size, sud) =
    let solver = new Solver(size)
    solver.solve (sud)

let solveParallel(size, sud) =
    let solver = new Solver(size)
    solver.solveParallel (sud)
    