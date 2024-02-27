from __future__ import annotations



# @pytest.mark.parametrize(
#     "df, ft, res_full, res_valid, res_same",
#     [
#         (
#             pl.DataFrame({"a": [5, 6, 7, 8, 9]}),
#             [1, 0, -1],
#             pl.DataFrame({"a": pl.Series([5, 6, 2, 2, 2, -8, -9], dtype=pl.Float64)}),
#             pl.DataFrame({"a": pl.Series([2, 2, 2], dtype=pl.Float64)}),
#             pl.DataFrame({"a": pl.Series([6, 2, 2, 2, -8], dtype=pl.Float64)}),
#         ),
#     ],
# )
# def test_convolve(df, ft, res_full, res_valid, res_same):
#     res = df.select(pl.col("a").num.convolve(ft, mode="full"))

#     assert_frame_equal(res, res_full)

#     res = df.select(pl.col("a").num.convolve(ft, mode="valid"))

#     assert_frame_equal(res, res_valid)

#     res = df.select(pl.col("a").num.convolve(ft, mode="same"))

#     assert_frame_equal(res, res_same)


