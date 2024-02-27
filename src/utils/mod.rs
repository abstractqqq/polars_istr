// use polars::{
//     datatypes::{DataType, Field},
//     error::PolarsResult,
//     // frame::DataFrame,
//     lazy::dsl::FieldsMapper,
//     // series::Series,
// };

// // -------------------------------------------------------------------------------
// // Common, Resuable Functions
// // -------------------------------------------------------------------------------

// // Shared splitting method
// pub fn split_offsets(len: usize, n: usize) -> Vec<(usize, usize)> {
//     if n == 1 {
//         vec![(0, len)]
//     } else {
//         let chunk_size = len / n;
//         (0..n)
//             .map(|partition| {
//                 let offset = partition * chunk_size;
//                 let len = if partition == (n - 1) {
//                     len - offset
//                 } else {
//                     chunk_size
//                 };
//                 (partition * chunk_size, len)
//             })
//             .collect()
//     }
// }

// // -------------------------------------------------------------------------------
// // Common Output Types
// // -------------------------------------------------------------------------------
