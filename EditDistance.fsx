#load "EditDistance.fs"

open System
open System.Text

open ParallelAlgorithms.EditDistance

let rand = new Random()
let LEN = 10000

let generateRandomString (rand: Random) =
    let sb = new StringBuilder(LEN)
    for i=0 to LEN-1 do
        (int 'a' + rand.Next(0, 26)) |> char |> sb.Append |> ignore
    sb.ToString()

let s1 = generateRandomString rand
let s2 = generateRandomString rand

#time "on";;

let d1 = editDistance s1 s2;;
let d2 = editDistanceParallel s1 s2;;