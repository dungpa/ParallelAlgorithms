#load "BootyDivision.fs"

open ParallelAlgorithms.BootyDivision

let minValue = 30
let maxValue = 50
let numItems = 25
let level = 10
let rnd = new System.Random()
let itemValues = Array.init numItems (fun _ -> rnd.Next(minValue, maxValue+1));;

#time "on";;

let c1 = solve(itemValues);;
let c11 = solve2(itemValues, level);;
let c2 = solveParallel(itemValues, level);;

let c3 = solveBB(itemValues);;
let c4 = solveBBParallel(itemValues, level);;
