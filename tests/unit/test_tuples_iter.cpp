/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "../../deps/googletest/include/gtest/gtest.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "../../deps/GraphBLAS/Include/GraphBLAS.h"
#include "../../src/util/rmalloc.h"

#ifdef __cplusplus
}
#endif

class TuplesTest: public ::testing::Test {
  protected:
    static void SetUpTestCase() {
      // Use the malloc family for allocations
      Alloc_Reset();

      GrB_init(GrB_NONBLOCKING);
      GxB_Global_Option_set(GxB_FORMAT, GxB_BY_ROW); // all matrices in CSR format
      GxB_Global_Option_set(GxB_HYPER, GxB_NEVER_HYPER); // matrices are never hypersparse
    }

    static void TearDownTestCase() {
      GrB_finalize();
    }

    GrB_Matrix CreateSquareNByNDiagonalMatrix(GrB_Index n) {
      GrB_Matrix A = CreateSquareNByNEmptyMatrix(n);

      GrB_Index I[n];
      GrB_Index J[n];
      bool X[n];

      // Initialize.
      for(int i = 0; i < n; i++) {
        I[i] = i;
        J[i] = i;
        X[i] = i;
      }

      GrB_Matrix_build_BOOL(A, I, J, X, n, GrB_FIRST_BOOL);
      
      return A;
    }

    GrB_Matrix CreateSquareNByNEmptyMatrix(GrB_Index n) {
      GrB_Matrix A;
      GrB_Matrix_new(&A, GrB_BOOL, n, n);
      return A;
    }
};

TEST_F(TuplesTest, RandomVectorTest) {
  //--------------------------------------------------------------------------
  // Build a random vector
  //--------------------------------------------------------------------------

  GrB_Vector A;
  GrB_Index nvals = 0;
  GrB_Index nrows = 1024;
  GrB_Index *I = (GrB_Index *)malloc(sizeof(GrB_Index) * nrows);
  bool *X = (bool *)malloc(sizeof(bool) * nrows);

  GrB_Vector_new(&A, GrB_BOOL, nrows);

  double mid_point = RAND_MAX/2;
  for(int i = 0; i < nrows; i++) {
    if(rand() > mid_point) {
      I[nvals] = i;
      X[nvals] = true;
      nvals++;
    }
  }

  GrB_Vector_build_BOOL(A, I, X, nvals, GrB_FIRST_BOOL);

  GrB_Index I_expected[nvals];
  GrB_Vector_extractTuples_BOOL(I_expected, NULL, &nvals, A);

  //--------------------------------------------------------------------------
  // Get an iterator over all nonzero elements.
  //--------------------------------------------------------------------------

  GxB_MatrixTupleIter *iter;
  GxB_MatrixTupleIter_new(&iter, (GrB_Matrix)A);
  GrB_Index col;

  //--------------------------------------------------------------------------
  // Verify iterator returned values.
  //--------------------------------------------------------------------------
  bool depleted = false;
  for(int i = 0; i < nvals; i++) {
    GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
    ASSERT_FALSE(depleted);
    ASSERT_EQ(col, I_expected[i]);
  }
  GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
  ASSERT_TRUE(depleted);

  //--------------------------------------------------------------------------
  // Clean up.
  //--------------------------------------------------------------------------
  free(I);
  free(X);
  GxB_MatrixTupleIter_free(iter);
  GrB_Vector_free(&A);
}

TEST_F(TuplesTest, VectorIteratorTest) {
  //--------------------------------------------------------------------------
  // Build a vector
  //--------------------------------------------------------------------------

  GrB_Vector A;
  GrB_Vector_new(&A, GrB_BOOL, 4);

  GrB_Index nvals = 2;
  GrB_Index I[2] = {1, 3};
  bool X[2] = {true, true};
  GrB_Index I_expected[nvals];

  GrB_Vector_build_BOOL(A, I, X, nvals, GrB_FIRST_BOOL);
  GrB_Vector_extractTuples_BOOL(I_expected, NULL, &nvals, A);

  //--------------------------------------------------------------------------
  // Get an iterator over all vector nonzero elements.
  //--------------------------------------------------------------------------

  GxB_MatrixTupleIter *iter;
  GxB_MatrixTupleIter_new(&iter, (GrB_Matrix)A);
  GrB_Index col;

  //--------------------------------------------------------------------------
  // Verify iterator returned values.
  //--------------------------------------------------------------------------
  bool depleted = false;
  for(int i = 0; i < nvals; i++) {
    GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
    ASSERT_FALSE(depleted);
    ASSERT_EQ(col, I_expected[i]);
  }
  GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
  ASSERT_TRUE(depleted);

  //--------------------------------------------------------------------------
  // Reset iterator and re-verify.
  //--------------------------------------------------------------------------

  GxB_MatrixTupleIter_reset(iter);
  for(int i = 0; i < nvals; i++) {
    GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
    ASSERT_FALSE(depleted);
    ASSERT_EQ(col, I_expected[i]);
  }
  GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
  ASSERT_TRUE(depleted);

  //--------------------------------------------------------------------------
  // Clean up.
  //--------------------------------------------------------------------------
  GxB_MatrixTupleIter_free(iter);
  GrB_Vector_free(&A);
}

TEST_F(TuplesTest, RandomMatrixTest) {
  //--------------------------------------------------------------------------
  // Build a random matrix
  //--------------------------------------------------------------------------

  GrB_Matrix A;
  GrB_Index nvals = 0;
  GrB_Index nrows = 1024;
  GrB_Index ncols = 1024;
  GrB_Index *I = (GrB_Index *)malloc(sizeof(GrB_Index) * ncols * nrows);
  GrB_Index *J = (GrB_Index *)malloc(sizeof(GrB_Index) * ncols * nrows);
  bool *X = (bool *)malloc(sizeof(bool) * ncols * nrows);

  GrB_Matrix_new(&A, GrB_BOOL, nrows, ncols);

  double mid_point = RAND_MAX/2;
  for(int i = 0; i < nrows; i++) {
    for(int j = 0; j < ncols; j++) {
      if(rand() > mid_point) {
        I[nvals] = i;
        J[nvals] = j;
        X[nvals] = true;
        nvals++;
      }
    }
  }
  GrB_Matrix_build_BOOL(A, I, J, X, nvals, GrB_FIRST_BOOL);

  GrB_Index *I_expected = (GrB_Index *)malloc(sizeof(GrB_Index) * nvals);
  GrB_Index *J_expected = (GrB_Index *)malloc(sizeof(GrB_Index) * nvals);
  GrB_Matrix_extractTuples_BOOL(I_expected, J_expected, NULL, &nvals, A);

  //--------------------------------------------------------------------------
  // Get an iterator over all matrix nonzero elements.
  //--------------------------------------------------------------------------

  GxB_MatrixTupleIter *iter;
  GxB_MatrixTupleIter_new(&iter, A);
  GrB_Index row;
  GrB_Index col;

  //--------------------------------------------------------------------------
  // Verify iterator returned values.
  //--------------------------------------------------------------------------
  bool depleted = false;
  for(int i = 0; i < nvals; i++) {
    GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
    ASSERT_FALSE(depleted);
    ASSERT_EQ(row, I_expected[i]);
    ASSERT_EQ(col, J_expected[i]);
  }
  GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
  ASSERT_TRUE(depleted);

  //--------------------------------------------------------------------------
  // Clean up.
  //--------------------------------------------------------------------------
  free(I);
  free(J);
  free(X);
  free(I_expected);
  free(J_expected);
  GxB_MatrixTupleIter_free(iter);
  GrB_Matrix_free(&A);
}

TEST_F(TuplesTest, MatrixIteratorTest) {
  //--------------------------------------------------------------------------
  // Build a 4X4 matrix
  //--------------------------------------------------------------------------

  GrB_Index nvals = 4;
  GrB_Matrix A = CreateSquareNByNDiagonalMatrix(nvals);
  GrB_Index I_expected[nvals];
  GrB_Index J_expected[nvals];
  GrB_Matrix_extractTuples_BOOL(I_expected, J_expected, NULL, &nvals, A);

  //--------------------------------------------------------------------------
  // Get an iterator over all matrix nonzero elements.
  //--------------------------------------------------------------------------

  GxB_MatrixTupleIter *iter;
  GxB_MatrixTupleIter_new(&iter, A);
  GrB_Index row;
  GrB_Index col;

  //--------------------------------------------------------------------------
  // Verify iterator returned values.
  //--------------------------------------------------------------------------
  bool depleted = false;
  for(int i = 0; i < nvals; i++) {
    GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
    ASSERT_FALSE(depleted);
    ASSERT_EQ(row, I_expected[i]);
    ASSERT_EQ(col, J_expected[i]);
  }
  GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
  ASSERT_TRUE(depleted);

  //--------------------------------------------------------------------------
  // Reset iterator an re-verify.
  //--------------------------------------------------------------------------

  GxB_MatrixTupleIter_reset(iter);
  for(int i = 0; i < nvals; i++) {
    GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
    ASSERT_FALSE(depleted);
    ASSERT_EQ(row, I_expected[i]);
    ASSERT_EQ(col, J_expected[i]);
  }
  GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
  ASSERT_TRUE(depleted);

  //--------------------------------------------------------------------------
  // Clean up.
  //--------------------------------------------------------------------------
  GxB_MatrixTupleIter_free(iter);
  GrB_Matrix_free(&A);
}

TEST_F(TuplesTest, ColumnIteratorTest) {
  //--------------------------------------------------------------------------
  // Build a 4X4 matrix
  //--------------------------------------------------------------------------

  GrB_Index nvals = 4;
  GrB_Matrix A = CreateSquareNByNDiagonalMatrix(nvals);
  GrB_Index I_expected[nvals];
  GrB_Vector v;
  GrB_Index row;
  GrB_Index col;
  GrB_Index nrows = nvals;
  GrB_Index ncols = nvals;
  GxB_MatrixTupleIter *iter;
  GxB_MatrixTupleIter_new(&iter, A);

  for(int j = 0; j < ncols; j++) {
    GrB_Vector_new(&v, GrB_BOOL, nrows);
    GrB_Col_extract(v, NULL, NULL, A, GrB_ALL, nrows, j, NULL);
    GrB_Vector_extractTuples_BOOL(I_expected, NULL, &nvals, v);

    //--------------------------------------------------------------------------
    // Test iterating over each column twice, this is to check
    // iterator reusability.
    //--------------------------------------------------------------------------
    
    int reuse = 2;
    for(int k = 0; k < reuse; k++) {
      //--------------------------------------------------------------------------
      // Get an iterator over the current column.
      //--------------------------------------------------------------------------
      GxB_MatrixTupleIter_iterate_row(iter, j);

      //--------------------------------------------------------------------------
      // Verify iterator returned values.
      //--------------------------------------------------------------------------
      bool depleted = false;
      for(int i = 0; i < nvals; i++) {
        GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
        ASSERT_FALSE(depleted);
        ASSERT_EQ(row, I_expected[i]);
        ASSERT_EQ(col, j);
      }
      GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
      ASSERT_TRUE(depleted);
    }

    GrB_Vector_free(&v);
  }
  GxB_MatrixTupleIter_free(iter);
  GrB_Matrix_free(&A);
}

TEST_F(TuplesTest, ColumnIteratorEmptyMatrixTest) {
    //--------------------------------------------------------------------------
    // Build a 4X4 empty matrix
    //--------------------------------------------------------------------------

    GrB_Index nvals = 4;
    GrB_Matrix A = CreateSquareNByNEmptyMatrix(nvals);
    GrB_Index row;
    GrB_Index col;
    GrB_Index ncols = nvals;
    GxB_MatrixTupleIter *iter;
    GxB_MatrixTupleIter_new(&iter, A);

    for(int j = 0; j < ncols; j++) {      

      //--------------------------------------------------------------------------
      // Get an iterator over the current column.
      //--------------------------------------------------------------------------
      GxB_MatrixTupleIter_iterate_row(iter, j);

      //--------------------------------------------------------------------------
      // Verify iterator returned values.
      //--------------------------------------------------------------------------
      bool depleted = false;
      GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
      ASSERT_TRUE(depleted);
    }

    GxB_MatrixTupleIter_free(iter);
    GrB_Matrix_free(&A);
}