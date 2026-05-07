# R implementations for the table stdlib module.
# Thin wrappers over arrow::RecordBatch operations. The wire-format layer
# carries Arrow C Data Interface structs across pool boundaries; the R
# arrow package's $from_record_batches / $export_to_c handle the binding.


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

morloc_asCol <- function(colname, vec) {
  # Build a single-column record batch carrying `vec` under `colname`.
  # `vec` arrives as an R list (the wire form of morloc's Vector n a in R);
  # unlist flattens it to the primitive R vector arrow infers a type from.
  flat <- if (length(vec) == 0L) vec else unlist(vec)
  do.call(arrow::record_batch, setNames(list(flat), colname))
}


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

morloc_nrow <- function(t) as.integer(t$num_rows)
morloc_ncol <- function(t) as.integer(t$num_columns)
morloc_names <- function(t) as.character(t$names())


# ---------------------------------------------------------------------------
# Row operations
# ---------------------------------------------------------------------------

morloc_sliceRows <- function(start, end, t) {
  if (start < 0) start <- 0
  if (end < 0) end <- 0
  total <- t$num_rows
  if (start >= total || end <= start) return(t$Slice(0, 0))
  end_clamped <- min(end, total)
  t$Slice(start, end_clamped - start)
}

morloc_filterRows <- function(mask, t) {
  flat <- if (length(mask) == 0L) logical(0) else as.logical(unlist(mask))
  if (length(flat) != t$num_rows) {
    stop("morloc_filterRows: mask length (", length(flat),
         ") does not match table row count (", t$num_rows, ")")
  }
  if (!any(flat)) {
    return(t$Slice(0, 0))
  }
  df <- as.data.frame(t)
  arrow::record_batch(df[flat, , drop = FALSE])
}

morloc_pickRows <- function(indices, t) {
  flat <- if (length(indices) == 0L) integer(0) else as.integer(unlist(indices))
  if (length(flat) == 0L) {
    return(t$Slice(0, 0))
  }
  # Convert from morloc 0-indexed to R 1-indexed.
  df <- as.data.frame(t)
  arrow::record_batch(df[flat + 1L, , drop = FALSE])
}

morloc_distinctRows <- function(t) {
  if (t$num_rows <= 1) return(t)
  df <- as.data.frame(t)
  arrow::record_batch(df[!duplicated(df), , drop = FALSE])
}

morloc_sortRows <- function(spec, t) {
  # spec is a list of (name, ascending) tuples; ascending = TRUE.
  if (length(spec) == 0L) return(t)
  df <- as.data.frame(t)
  # Build the order() argument list: each key becomes its column with
  # negative wrapping for descending where the data is numeric. For
  # non-numeric columns, descending requires `xtfrm + minus` or
  # explicit decreasing per-key, which R's `order` doesn't support
  # natively. Emulate per-column descending via rank-and-negate.
  cols <- list()
  for (i in seq_along(spec)) {
    pair <- spec[[i]]
    name <- pair[[1]]
    asc <- as.logical(pair[[2]])
    v <- df[[name]]
    if (asc) {
      cols[[i]] <- v
    } else if (is.numeric(v)) {
      cols[[i]] <- -v
    } else {
      cols[[i]] <- -xtfrm(v)
    }
  }
  ord <- do.call(order, cols)
  arrow::record_batch(df[ord, , drop = FALSE])
}


# ---------------------------------------------------------------------------
# Column operations
# ---------------------------------------------------------------------------

morloc_getCol <- function(name, t) {
  # Extract the named column as an R atomic vector. The wire form is morloc
  # Vector (= List); unlist normalizes the column to the appropriate R type.
  if (!(name %in% t$names())) {
    stop("morloc_getCol: column '", name, "' not found")
  }
  col <- t$column(match(name, t$names()) - 1L)
  as.vector(col)
}

morloc_setCol <- function(name, vec, t) {
  flat <- if (length(vec) == 0L) vec else unlist(vec)
  df <- as.data.frame(t)
  df[[name]] <- flat
  arrow::record_batch(df)
}

morloc_dropCols <- function(names, t) {
  keep <- setdiff(t$names(), names)
  if (length(keep) == 0L) {
    return(arrow::record_batch(data.frame()))
  }
  idx <- match(keep, t$names())
  t$SelectColumns(as.integer(idx - 1L))
}

morloc_selectCols <- function(names, t) {
  idx <- match(names, t$names())
  if (any(is.na(idx))) {
    missing <- names[is.na(idx)]
    stop("morloc_selectCols: column(s) not found: ", paste(missing, collapse = ", "))
  }
  t$SelectColumns(as.integer(idx - 1L))
}

morloc_selectColsDyn <- function(cols, t) {
  all_names <- t$names()
  idx <- match(cols, all_names)
  if (any(is.na(idx))) {
    missing <- cols[is.na(idx)]
    stop("morloc_selectColsDyn: column(s) not found: ", paste(missing, collapse = ", "))
  }
  t$SelectColumns(as.integer(idx - 1L))
}

morloc_renameCol <- function(old_name, new_name, t) {
  all_names <- t$names()
  idx <- match(old_name, all_names)
  if (is.na(idx)) {
    stop("morloc_renameCol: column '", old_name, "' not found")
  }
  new_names <- all_names
  new_names[idx] <- new_name
  df <- as.data.frame(t)
  names(df) <- new_names
  arrow::record_batch(df)
}


# ---------------------------------------------------------------------------
# Concatenation
# ---------------------------------------------------------------------------

morloc_rbind <- function(t1, t2) {
  arrow::record_batch(rbind(as.data.frame(t1), as.data.frame(t2)))
}

morloc_cbind <- function(t1, t2) {
  arrow::record_batch(cbind(as.data.frame(t1), as.data.frame(t2)))
}
