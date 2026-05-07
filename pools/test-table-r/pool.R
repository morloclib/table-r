# AUTO include sources start
dyn.load("/home/dev/.local/share/morloc/lib/librmorloc.so")
.morloc.srcdir <- normalizePath(file.path(dirname(sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value=TRUE)[1])), "..", ".."), mustWork=FALSE)
.morloc.source <- function(p) source(ifelse(startsWith(p, "/"), p, file.path(.morloc.srcdir, p)), chdir=TRUE)
.morloc.source("/home/dev/.local/share/morloc/src/morloc/plane/default/root-r/R/core.R")
.morloc.source("/home/dev/.local/share/morloc/src/morloc/plane/default/table-r/R/table.R")
mlc_schema_table = list( "<list>t2<integer>j<integer>j"
, "<logical>b"
, "<logical>a:2<integer>j"
, "<logical>a:3<integer>j"
, "T:11x<integer>j"
, "<numeric>a:2<integer>j"
, "<numeric>a:3<integer>j"
, "<logical>a:6<integer>j"
, "<numeric>a:6<integer>j"
, "<integer>j"
, "T:21x<integer>j1y<character>s"
, "T:11a<integer>j"
, "T:11y<character>s"
, "<logical>a:3<numeric>f8"
, "<numeric>a:3<numeric>f8"
, "T:31x<integer>j1y<character>s1z<numeric>f8"
, "<character>a:3<character>s"
, "<logical>a:5<integer>j"
, "<numeric>a:5<integer>j"
, "<logical>a:1<integer>j"
, "<numeric>a:1<integer>j"
, "<logical>a:3<logical>b"
, "<list>a<character>s" )
# AUTO include sources end

morloc_is_ping                       <- function(...){ .Call("morloc_is_ping",                       ...) }
morloc_pong                          <- function(...){ .Call("morloc_pong",                          ...) }
morloc_is_local_call                 <- function(...){ .Call("morloc_is_local_call",                 ...) }
morloc_is_remote_call                <- function(...){ .Call("morloc_is_remote_call",                ...) }
morloc_make_fail_packet              <- function(...){ .Call("morloc_make_fail_packet",              ...) }
morloc_wait_for_client               <- function(...){ .Call("morloc_wait_for_client",               ...) }
morloc_stream_from_client            <- function(...){ .Call("morloc_stream_from_client",            ...) }
morloc_read_morloc_call_packet       <- function(...){ .Call("morloc_read_morloc_call_packet",       ...) }
morloc_send_packet_to_foreign_server <- function(...){ .Call("morloc_send_packet_to_foreign_server", ...) }
morloc_close_socket                  <- function(...){ .Call("morloc_close_socket",                  ...) }
morloc_start_daemon                  <- function(...){ .Call("morloc_start_daemon",                  ...) }
morloc_shinit                        <- function(...){ .Call("morloc_shinit",                        ...) }
morloc_foreign_call                  <- function(...){ .Call("morloc_foreign_call",                  ...) }
morloc_get_value                     <- function(...){ .Call("morloc_get_value",                     ...) }
morloc_put_value                     <- function(...){ .Call("morloc_put_value",                     ...) }
morloc_mlc_show                      <- function(...){ .Call("morloc_mlc_show",                      ...) }
morloc_socketpair                    <- function(...){ .Call("morloc_socketpair",                    ...) }
morloc_fork                          <- function(...){ .Call("morloc_fork",                          ...) }
morloc_send_fd                       <- function(...){ .Call("morloc_send_fd",                       ...) }
morloc_recv_fd                       <- function(...){ .Call("morloc_recv_fd",                       ...) }
morloc_kill                          <- function(...){ .Call("morloc_kill",                          ...) }
morloc_waitpid                       <- function(...){ .Call("morloc_waitpid",                       ...) }
morloc_install_sigterm_handler       <- function(...){ .Call("morloc_install_sigterm_handler",       ...) }
morloc_is_shutting_down              <- function(...){ .Call("morloc_is_shutting_down",              ...) }
morloc_waitpid_blocking              <- function(...){ .Call("morloc_waitpid_blocking",              ...) }
morloc_detach_daemon                 <- function(...){ .Call("morloc_detach_daemon",                 ...) }
morloc_shared_counter_create         <- function(...){ .Call("morloc_shared_counter_create",         ...) }
morloc_shared_counter_inc            <- function(...){ .Call("morloc_shared_counter_inc",            ...) }
morloc_shared_counter_dec            <- function(...){ .Call("morloc_shared_counter_dec",            ...) }
morloc_shared_counter_read           <- function(...){ .Call("morloc_shared_counter_read",           ...) }
morloc_pipe                          <- function(...){ .Call("morloc_pipe",                          ...) }
morloc_write_byte                    <- function(...){ .Call("morloc_write_byte",                    ...) }
morloc_close_fd                      <- function(...){ .Call("morloc_close_fd",                      ...) }
morloc_worker_loop_c                 <- function(...){ .Call("morloc_worker_loop_c",                 ...) }
morloc_set_line_buffered             <- function(...){ .Call("morloc_set_line_buffered",             ...) }
morloc_exit                          <- function(...){ .Call("morloc_exit",                          ...) }

global_state <- list()

# Dynamic worker spawning: monkey-patch morloc_foreign_call to track busy workers.
# Workers atomically increment a shared counter before a foreign_call and
# decrement after. When all workers are busy, a byte is written to a wake-up
# pipe to tell the dispatcher to spawn a new worker.
.orig_foreign_call <- morloc_foreign_call
.busy_counter <- NULL
.wakeup_fd <- NULL
.n_workers_total <- 0L

morloc_foreign_call <- function(...) {
  val <- morloc_shared_counter_inc(.busy_counter)
  if (val >= .n_workers_total && !is.null(.wakeup_fd)) {
    tryCatch(morloc_write_byte(.wakeup_fd, as.raw(0x21)), error = function(e) NULL)
  }
  on.exit(morloc_shared_counter_dec(.busy_counter))
  .orig_foreign_call(...)
}

# AUTO include manifolds start
m2839 <- function()
{
    s1 <- morloc_foreign_call( paste0(global_state$tmpdir, "/", "pipe-py")
    , 2839L
    , list() )
    return(morloc_get_value(s1, mlc_schema_table[[1]]))
}

m2846 <- function()
{
    n2 <- m2839()[[1]]
    return(n2)
}

m1 <- function()
{
    n3 <- morloc_eq(0L, m2846())
    return(morloc_put_value(n3, mlc_schema_table[[2]]))
}
m2739 <- function()
{
    n1 <- list(0L, 0L)
    return(morloc_put_value(n1, mlc_schema_table[[1]]))
}
m2875 <- function()
{
    n1 <- c(0L, 1L)
    return(morloc_put_value(n1, mlc_schema_table[[3]]))
}
m2878 <- function()
{
    n1 <- c(1L, 2L)
    return(morloc_put_value(n1, mlc_schema_table[[3]]))
}
m2881 <- function()
{
    n1 <- c(2L, 1L, 0L)
    return(morloc_put_value(n1, mlc_schema_table[[4]]))
}
m2885 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m2893 <- function(s3)
{
    n3 <- morloc_get_value(s3, mlc_schema_table[[5]])
    n4 <- morloc_sliceRows(0L, 2L, n3)
    return(morloc_put_value(n4, mlc_schema_table[[5]]))
}
m2898 <- function(s0)
{
    n0 <- morloc_get_value(s0, mlc_schema_table[[6]])
    n1 <- morloc_asCol("x", n0)
    return(morloc_put_value(n1, mlc_schema_table[[5]]))
}
m2911 <- function(n3)
{
    n4 <- morloc_nrow(n3)
    return(n4)
}

m2915 <- function(n3)
{
    n6 <- morloc_nrow(n3)
    return(n6)
}

m2907 <- function(s3)
{
    n3 <- morloc_get_value(s3, mlc_schema_table[[5]])
    n5 <- (m2911(n3) - 2L)
    n7 <- morloc_sliceRows(n5, m2915(n3), n3)
    return(morloc_put_value(n7, mlc_schema_table[[5]]))
}
m2919 <- function(s1)
{
    n1 <- morloc_get_value(s1, mlc_schema_table[[6]])
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m2935 <- function(n3)
{
    n4 <- morloc_nrow(n3)
    return(n4)
}

m2930 <- function(n3)
{
    n5 <- (m2935(n3) - 1L)
    n6 <- morloc_range(0L, n5)
    return(n6)
}

m2928 <- function(n3)
{
    n7 <- rev(m2930(n3))
    return(n7)
}

m2926 <- function(s3)
{
    n3 <- morloc_get_value(s3, mlc_schema_table[[5]])
    n8 <- morloc_pickRows(m2928(n3), n3)
    return(morloc_put_value(n8, mlc_schema_table[[5]]))
}
m2940 <- function(s2)
{
    n2 <- morloc_get_value(s2, mlc_schema_table[[7]])
    n3 <- morloc_asCol("x", n2)
    return(morloc_put_value(n3, mlc_schema_table[[5]]))
}
m2985 <- function()
{
    n1 <- c(0L, 1L, 2L, 0L, 1L, 2L)
    return(morloc_put_value(n1, mlc_schema_table[[8]]))
}
m2992 <- function(s4)
{
    n4 <- morloc_get_value(s4, mlc_schema_table[[9]])
    n5 <- morloc_asCol("x", n4)
    return(morloc_put_value(n5, mlc_schema_table[[5]]))
}
m3005 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m3008 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m3003 <- function()
{
    n5 <- morloc_rbind(m3005(), m3008())
    return(n5)
}

m3001 <- function()
{
    n6 <- morloc_nrow(m3003())
    return(morloc_put_value(n6, mlc_schema_table[[10]]))
}
m3019 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m3022 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m3017 <- function()
{
    n5 <- morloc_rbind(m3019(), m3022())
    return(morloc_put_value(n5, mlc_schema_table[[5]]))
}
m3035 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m3038 <- function()
{
    n3 <- c("0", "1", "2")
    n4 <- morloc_asCol("y", n3)
    return(n4)
}

m3033 <- function()
{
    n5 <- morloc_cbind(m3035(), m3038())
    return(n5)
}

m3031 <- function()
{
    n6 <- morloc_ncol(m3033())
    return(morloc_put_value(n6, mlc_schema_table[[10]]))
}
m3047 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m3050 <- function()
{
    n3 <- c("0", "1", "2")
    n4 <- morloc_asCol("y", n3)
    return(n4)
}

m3045 <- function()
{
    n5 <- morloc_cbind(m3047(), m3050())
    return(morloc_put_value(n5, mlc_schema_table[[11]]))
}
m3151 <- function()
{
    n2 <- c(0L, 1L, 2L)
    n3 <- morloc_asCol("x", n2)
    return(n3)
}

m3053 <- function()
{
    n1 <- c("0", "1", "2")
    n4 <- morloc_setCol("y", n1, m3151())
    return(morloc_put_value(n4, mlc_schema_table[[11]]))
}
m3191 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m3187 <- function()
{
    n3 <- morloc_renameCol("x", "a", m3191())
    return(morloc_put_value(n3, mlc_schema_table[[12]]))
}
m3194 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("a", n1)
    return(morloc_put_value(n2, mlc_schema_table[[12]]))
}
m3208 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m3204 <- function()
{
    n3 <- morloc_renameCol("x", "a", m3208())
    return(n3)
}

m3201 <- function()
{
    n4 <- morloc_getCol("a", m3204())
    return(morloc_put_value(n4, mlc_schema_table[[7]]))
}
m3302 <- function()
{
    n4 <- c(0L, 1L, 2L)
    n5 <- morloc_asCol("x", n4)
    return(n5)
}

m3298 <- function()
{
    n3 <- c("0", "1", "2")
    n6 <- morloc_setCol("y", n3, m3302())
    return(n6)
}

m3272 <- function()
{
    n2 <- c(0.0, 0.5, 1.0)
    n7 <- morloc_setCol("z", n2, m3298())
    return(n7)
}

m3268 <- function()
{
    n1 <- list("x")
    n8 <- morloc_selectColsDyn(n1, m3272())
    return(morloc_put_value(n8, mlc_schema_table[[5]]))
}
m3273 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m3335 <- function()
{
    n4 <- c(0L, 1L, 2L)
    n5 <- morloc_asCol("x", n4)
    return(n5)
}

m3331 <- function()
{
    n3 <- c("0", "1", "2")
    n6 <- morloc_setCol("y", n3, m3335())
    return(n6)
}

m3286 <- function()
{
    n2 <- c(0.0, 0.5, 1.0)
    n7 <- morloc_setCol("z", n2, m3331())
    return(n7)
}

m3281 <- function()
{
    n1 <- list("y", "z")
    n8 <- morloc_selectColsDyn(n1, m3286())
    return(n8)
}

m3279 <- function()
{
    n9 <- morloc_ncol(m3281())
    return(morloc_put_value(n9, mlc_schema_table[[10]]))
}
m3423 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m3379 <- function()
{
    n2 <- c("0", "1", "2")
    n5 <- morloc_setCol("y", n2, m3423())
    return(n5)
}

m3375 <- function()
{
    n1 <- list("x")
    n6 <- morloc_selectCols(n1, m3379())
    return(morloc_put_value(n6, mlc_schema_table[[5]]))
}
m3382 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m3455 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m3394 <- function()
{
    n2 <- c("0", "1", "2")
    n5 <- morloc_setCol("y", n2, m3455())
    return(n5)
}

m3390 <- function()
{
    n1 <- list("y")
    n6 <- morloc_selectCols(n1, m3394())
    return(morloc_put_value(n6, mlc_schema_table[[13]]))
}
m3397 <- function()
{
    n1 <- c("0", "1", "2")
    n2 <- morloc_asCol("y", n1)
    return(morloc_put_value(n2, mlc_schema_table[[13]]))
}
m3484 <- function()
{
    n4 <- c(0L, 1L, 2L)
    n5 <- morloc_asCol("x", n4)
    return(n5)
}

m3480 <- function()
{
    n3 <- c("0", "1", "2")
    n6 <- morloc_setCol("y", n3, m3484())
    return(n6)
}

m3408 <- function()
{
    n2 <- c(0.0, 0.5, 1.0)
    n7 <- morloc_setCol("z", n2, m3480())
    return(n7)
}

m3403 <- function()
{
    n1 <- list("x", "y")
    n8 <- morloc_selectCols(n1, m3408())
    return(morloc_put_value(n8, mlc_schema_table[[11]]))
}
m3505 <- function()
{
    n2 <- c(0L, 1L, 2L)
    n3 <- morloc_asCol("x", n2)
    return(n3)
}

m3409 <- function()
{
    n1 <- c("0", "1", "2")
    n4 <- morloc_setCol("y", n1, m3505())
    return(morloc_put_value(n4, mlc_schema_table[[11]]))
}
m3590 <- function()
{
    n4 <- c(0L, 1L, 2L)
    n5 <- morloc_asCol("x", n4)
    return(n5)
}

m3586 <- function()
{
    n3 <- c("0", "1", "2")
    n6 <- morloc_setCol("y", n3, m3590())
    return(n6)
}

m3545 <- function()
{
    n2 <- c(0.0, 0.5, 1.0)
    n7 <- morloc_setCol("z", n2, m3586())
    return(n7)
}

m3541 <- function()
{
    n1 <- list("z")
    n8 <- morloc_dropCols(n1, m3545())
    return(morloc_put_value(n8, mlc_schema_table[[11]]))
}
m3611 <- function()
{
    n2 <- c(0L, 1L, 2L)
    n3 <- morloc_asCol("x", n2)
    return(n3)
}

m3546 <- function()
{
    n1 <- c("0", "1", "2")
    n4 <- morloc_setCol("y", n1, m3611())
    return(morloc_put_value(n4, mlc_schema_table[[11]]))
}
m3637 <- function()
{
    n4 <- c(0L, 1L, 2L)
    n5 <- morloc_asCol("x", n4)
    return(n5)
}

m3633 <- function()
{
    n3 <- c("0", "1", "2")
    n6 <- morloc_setCol("y", n3, m3637())
    return(n6)
}

m3559 <- function()
{
    n2 <- c(0.0, 0.5, 1.0)
    n7 <- morloc_setCol("z", n2, m3633())
    return(n7)
}

m3554 <- function()
{
    n1 <- list("x", "z")
    n8 <- morloc_dropCols(n1, m3559())
    return(morloc_put_value(n8, mlc_schema_table[[13]]))
}
m3560 <- function()
{
    n1 <- c("0", "1", "2")
    n2 <- morloc_asCol("y", n1)
    return(morloc_put_value(n2, mlc_schema_table[[13]]))
}
m3570 <- function()
{
    n2 <- c(0L, 1L, 2L)
    n3 <- morloc_asCol("x", n2)
    return(n3)
}

m3566 <- function()
{
    n1 <- list("absent")
    n4 <- morloc_dropCols(n1, m3570())
    return(morloc_put_value(n4, mlc_schema_table[[5]]))
}
m3573 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m3700 <- function()
{
    n1 <- c(9.0, 9.0, 9.0)
    return(morloc_put_value(n1, mlc_schema_table[[14]]))
}
m3757 <- function()
{
    n9 <- c(0L, 1L, 2L)
    n10 <- morloc_asCol("x", n9)
    return(n10)
}

m3753 <- function()
{
    n8 <- c("0", "1", "2")
    n11 <- morloc_setCol("y", n8, m3757())
    return(n11)
}

m3708 <- function()
{
    n7 <- c(0.0, 0.5, 1.0)
    n12 <- morloc_setCol("z", n7, m3753())
    return(n12)
}

m3704 <- function(s6)
{
    n6 <- morloc_get_value(s6, mlc_schema_table[[15]])
    n13 <- morloc_setCol("z", n6, m3708())
    return(morloc_put_value(n13, mlc_schema_table[[16]]))
}
m3778 <- function()
{
    n8 <- c(0L, 1L, 2L)
    n9 <- morloc_asCol("x", n8)
    return(n9)
}

m3713 <- function()
{
    n7 <- c("0", "1", "2")
    n10 <- morloc_setCol("y", n7, m3778())
    return(n10)
}

m3709 <- function(s6)
{
    n6 <- morloc_get_value(s6, mlc_schema_table[[15]])
    n11 <- morloc_setCol("z", n6, m3713())
    return(morloc_put_value(n11, mlc_schema_table[[16]]))
}
m3725 <- function()
{
    n2 <- c(0L, 1L, 2L)
    n3 <- morloc_asCol("x", n2)
    return(n3)
}

m3721 <- function()
{
    n1 <- c("0", "1", "2")
    n4 <- morloc_setCol("y", n1, m3725())
    return(morloc_put_value(n4, mlc_schema_table[[11]]))
}
m3814 <- function()
{
    n2 <- c(0L, 1L, 2L)
    n3 <- morloc_asCol("x", n2)
    return(n3)
}

m3728 <- function()
{
    n1 <- c("0", "1", "2")
    n4 <- morloc_setCol("y", n1, m3814())
    return(morloc_put_value(n4, mlc_schema_table[[11]]))
}
m3841 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m3740 <- function()
{
    n2 <- c("0", "1", "2")
    n5 <- morloc_setCol("y", n2, m3841())
    return(n5)
}

m3736 <- function()
{
    n1 <- c(0.0, 0.5, 1.0)
    n6 <- morloc_setCol("z", n1, m3740())
    return(morloc_put_value(n6, mlc_schema_table[[16]]))
}
m3861 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m3857 <- function()
{
    n2 <- c("0", "1", "2")
    n5 <- morloc_setCol("y", n2, m3861())
    return(n5)
}

m3743 <- function()
{
    n1 <- c(0.0, 0.5, 1.0)
    n6 <- morloc_setCol("z", n1, m3857())
    return(morloc_put_value(n6, mlc_schema_table[[16]]))
}
m3904 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m3901 <- function()
{
    n3 <- morloc_getCol("x", m3904())
    return(morloc_put_value(n3, mlc_schema_table[[7]]))
}
m3959 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m3955 <- function()
{
    n2 <- c("0", "1", "2")
    n5 <- morloc_setCol("y", n2, m3959())
    return(n5)
}

m3916 <- function()
{
    n1 <- c(0.0, 0.5, 1.0)
    n6 <- morloc_setCol("z", n1, m3955())
    return(n6)
}

m3913 <- function()
{
    n7 <- morloc_getCol("y", m3916())
    return(morloc_put_value(n7, mlc_schema_table[[17]]))
}
m3987 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m3983 <- function()
{
    n2 <- c("0", "1", "2")
    n5 <- morloc_setCol("y", n2, m3987())
    return(n5)
}

m3924 <- function()
{
    n1 <- c(0.0, 0.5, 1.0)
    n6 <- morloc_setCol("z", n1, m3983())
    return(n6)
}

m3921 <- function()
{
    n7 <- morloc_getCol("z", m3924())
    return(morloc_put_value(n7, mlc_schema_table[[15]]))
}
m4042 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m4036 <- function()
{
    n1 <- list("x", TRUE)
    n2 <- list(n1)
    n5 <- morloc_sortRows(n2, m4042())
    return(morloc_put_value(n5, mlc_schema_table[[5]]))
}
m4045 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m4059 <- function(n9)
{
    n12 <- morloc_asCol("x", n9)
    return(n12)
}

m4053 <- function(s9)
{
    n9 <- morloc_get_value(s9, mlc_schema_table[[7]])
    n10 <- list("x", TRUE)
    n11 <- list(n10)
    n13 <- morloc_sortRows(n11, m4059(n9))
    return(morloc_put_value(n13, mlc_schema_table[[5]]))
}
m4063 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m4077 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m4071 <- function()
{
    n1 <- list("x", FALSE)
    n2 <- list(n1)
    n5 <- morloc_sortRows(n2, m4077())
    return(morloc_put_value(n5, mlc_schema_table[[5]]))
}
m4080 <- function(s9)
{
    n9 <- morloc_get_value(s9, mlc_schema_table[[7]])
    n10 <- morloc_asCol("x", n9)
    return(morloc_put_value(n10, mlc_schema_table[[5]]))
}
m4090 <- function()
{
    n2 <- c(0L, 1L, 2L)
    n3 <- morloc_asCol("x", n2)
    return(n3)
}

m4087 <- function()
{
    n1 <- list()
    n4 <- morloc_sortRows(n1, m4090())
    return(morloc_put_value(n4, mlc_schema_table[[5]]))
}
m4093 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m4188 <- function()
{
    n1 <- c(7L, 7L, 7L)
    return(morloc_put_value(n1, mlc_schema_table[[4]]))
}
m4192 <- function(s10)
{
    n10 <- morloc_get_value(s10, mlc_schema_table[[7]])
    n11 <- morloc_asCol("x", n10)
    return(morloc_put_value(n11, mlc_schema_table[[5]]))
}
m4196 <- function()
{
    n1 <- c(1L, 2L, 1L, 3L, 2L)
    return(morloc_put_value(n1, mlc_schema_table[[18]]))
}
m4202 <- function(s12)
{
    n12 <- morloc_get_value(s12, mlc_schema_table[[19]])
    n13 <- morloc_asCol("x", n12)
    return(morloc_put_value(n13, mlc_schema_table[[5]]))
}
m4206 <- function()
{
    n1 <- c(1L, 2L, 3L)
    return(morloc_put_value(n1, mlc_schema_table[[4]]))
}
m4210 <- function(s14)
{
    n14 <- morloc_get_value(s14, mlc_schema_table[[7]])
    n15 <- morloc_asCol("x", n14)
    return(morloc_put_value(n15, mlc_schema_table[[5]]))
}
m4214 <- function()
{
    n1 <- c(7L)
    return(morloc_put_value(n1, mlc_schema_table[[20]]))
}
m4223 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m4221 <- function()
{
    n3 <- morloc_distinctRows(m4223())
    return(morloc_put_value(n3, mlc_schema_table[[5]]))
}
m4226 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m4234 <- function(s11)
{
    n11 <- morloc_get_value(s11, mlc_schema_table[[5]])
    n12 <- morloc_distinctRows(n11)
    return(morloc_put_value(n12, mlc_schema_table[[5]]))
}
m4237 <- function(s16)
{
    n16 <- morloc_get_value(s16, mlc_schema_table[[21]])
    n17 <- morloc_asCol("x", n16)
    return(morloc_put_value(n17, mlc_schema_table[[5]]))
}
m4244 <- function(s13)
{
    n13 <- morloc_get_value(s13, mlc_schema_table[[5]])
    n14 <- morloc_distinctRows(n13)
    return(morloc_put_value(n14, mlc_schema_table[[5]]))
}
m4336 <- function()
{
    n18 <- c(0L, 1L, 2L)
    n19 <- morloc_asCol("x", n18)
    return(n19)
}

m4333 <- function(s17)
{
    n17 <- morloc_get_value(s17, mlc_schema_table[[7]])
    n20 <- morloc_pickRows(n17, m4336())
    return(morloc_put_value(n20, mlc_schema_table[[5]]))
}
m4339 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m4350 <- function()
{
    n19 <- c(0L, 1L, 2L)
    n20 <- morloc_asCol("x", n19)
    return(n20)
}

m4347 <- function(s18)
{
    n18 <- morloc_get_value(s18, mlc_schema_table[[7]])
    n21 <- morloc_pickRows(n18, m4350())
    return(morloc_put_value(n21, mlc_schema_table[[5]]))
}
m4353 <- function(s21)
{
    n21 <- morloc_get_value(s21, mlc_schema_table[[7]])
    n22 <- morloc_asCol("x", n21)
    return(morloc_put_value(n22, mlc_schema_table[[5]]))
}
m4365 <- function()
{
    n20 <- c(0L, 1L, 2L)
    n21 <- morloc_asCol("x", n20)
    return(n21)
}

m4362 <- function(s19)
{
    n19 <- morloc_get_value(s19, mlc_schema_table[[9]])
    n22 <- morloc_pickRows(n19, m4365())
    return(morloc_put_value(n22, mlc_schema_table[[5]]))
}
m4368 <- function(s22)
{
    n22 <- morloc_get_value(s22, mlc_schema_table[[9]])
    n23 <- morloc_asCol("x", n22)
    return(morloc_put_value(n23, mlc_schema_table[[5]]))
}
m4378 <- function()
{
    n21 <- c(0L, 1L, 2L)
    n22 <- morloc_asCol("x", n21)
    return(n22)
}

m4375 <- function(s20)
{
    n20 <- morloc_get_value(s20, mlc_schema_table[[21]])
    n23 <- morloc_pickRows(n20, m4378())
    return(morloc_put_value(n23, mlc_schema_table[[5]]))
}
m4381 <- function(s23)
{
    n23 <- morloc_get_value(s23, mlc_schema_table[[21]])
    n24 <- morloc_asCol("x", n23)
    return(morloc_put_value(n24, mlc_schema_table[[5]]))
}
m4491 <- function()
{
    n25 <- c(0L, 1L, 2L)
    n26 <- morloc_asCol("x", n25)
    return(n26)
}

m4488 <- function(s24)
{
    n24 <- morloc_get_value(s24, mlc_schema_table[[22]])
    n27 <- morloc_filterRows(n24, m4491())
    return(morloc_put_value(n27, mlc_schema_table[[5]]))
}
m4494 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m4507 <- function()
{
    n26 <- c(0L, 1L, 2L)
    n27 <- morloc_asCol("x", n26)
    return(n27)
}

m4504 <- function(n25)
{
    n28 <- morloc_filterRows(n25, m4507())
    return(n28)
}

m4502 <- function(s25)
{
    n25 <- morloc_get_value(s25, mlc_schema_table[[22]])
    n29 <- morloc_nrow(m4504(n25))
    return(morloc_put_value(n29, mlc_schema_table[[10]]))
}
m4519 <- function()
{
    n27 <- c(0L, 1L, 2L)
    n28 <- morloc_asCol("x", n27)
    return(n28)
}

m4516 <- function(s26)
{
    n26 <- morloc_get_value(s26, mlc_schema_table[[22]])
    n29 <- morloc_filterRows(n26, m4519())
    return(morloc_put_value(n29, mlc_schema_table[[5]]))
}
m4522 <- function(s27)
{
    n27 <- morloc_get_value(s27, mlc_schema_table[[6]])
    n28 <- morloc_asCol("x", n27)
    return(morloc_put_value(n28, mlc_schema_table[[5]]))
}
m4534 <- function()
{
    n26 <- c(0L, 1L, 2L)
    n27 <- morloc_asCol("x", n26)
    return(n27)
}

m4531 <- function(n25)
{
    n28 <- morloc_filterRows(n25, m4534())
    return(n28)
}

m4529 <- function(s25)
{
    n25 <- morloc_get_value(s25, mlc_schema_table[[22]])
    n29 <- morloc_names(m4531(n25))
    return(morloc_put_value(n29, mlc_schema_table[[23]]))
}
m4622 <- function()
{
    n1 <- c(0L)
    return(morloc_put_value(n1, mlc_schema_table[[20]]))
}
m4624 <- function()
{
    n1 <- c(1L)
    return(morloc_put_value(n1, mlc_schema_table[[20]]))
}
m4626 <- function()
{
    n1 <- c(2L)
    return(morloc_put_value(n1, mlc_schema_table[[20]]))
}
m4628 <- function()
{
    n1 <- c(0L, 1L)
    return(morloc_put_value(n1, mlc_schema_table[[3]]))
}
m4631 <- function()
{
    n1 <- c(1L, 2L)
    return(morloc_put_value(n1, mlc_schema_table[[3]]))
}
m4634 <- function()
{
    n1 <- c(0L, 1L, 2L)
    return(morloc_put_value(n1, mlc_schema_table[[4]]))
}
m4638 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m4648 <- function(n34)
{
    n35 <- morloc_sliceRows(0L, 0L, n34)
    return(n35)
}

m4646 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n36 <- morloc_nrow(m4648(n34))
    return(morloc_put_value(n36, mlc_schema_table[[10]]))
}
m4661 <- function(n34)
{
    n35 <- morloc_sliceRows(0L, 0L, n34)
    return(n35)
}

m4659 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n36 <- morloc_names(m4661(n34))
    return(morloc_put_value(n36, mlc_schema_table[[23]]))
}
m4673 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n35 <- morloc_sliceRows(0L, 1L, n34)
    return(morloc_put_value(n35, mlc_schema_table[[5]]))
}
m4678 <- function(s28)
{
    n28 <- morloc_get_value(s28, mlc_schema_table[[21]])
    n29 <- morloc_asCol("x", n28)
    return(morloc_put_value(n29, mlc_schema_table[[5]]))
}
m4687 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n35 <- morloc_sliceRows(0L, 2L, n34)
    return(morloc_put_value(n35, mlc_schema_table[[5]]))
}
m4692 <- function(s31)
{
    n31 <- morloc_get_value(s31, mlc_schema_table[[6]])
    n32 <- morloc_asCol("x", n31)
    return(morloc_put_value(n32, mlc_schema_table[[5]]))
}
m4701 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n35 <- morloc_sliceRows(0L, 3L, n34)
    return(morloc_put_value(n35, mlc_schema_table[[5]]))
}
m4706 <- function(s33)
{
    n33 <- morloc_get_value(s33, mlc_schema_table[[7]])
    n34 <- morloc_asCol("x", n33)
    return(morloc_put_value(n34, mlc_schema_table[[5]]))
}
m4717 <- function(n34)
{
    n35 <- morloc_sliceRows(1L, 0L, n34)
    return(n35)
}

m4715 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n36 <- morloc_nrow(m4717(n34))
    return(morloc_put_value(n36, mlc_schema_table[[10]]))
}
m4730 <- function(n34)
{
    n35 <- morloc_sliceRows(1L, 1L, n34)
    return(n35)
}

m4728 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n36 <- morloc_nrow(m4730(n34))
    return(morloc_put_value(n36, mlc_schema_table[[10]]))
}
m4741 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n35 <- morloc_sliceRows(1L, 2L, n34)
    return(morloc_put_value(n35, mlc_schema_table[[5]]))
}
m4746 <- function(s29)
{
    n29 <- morloc_get_value(s29, mlc_schema_table[[21]])
    n30 <- morloc_asCol("x", n29)
    return(morloc_put_value(n30, mlc_schema_table[[5]]))
}
m4755 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n35 <- morloc_sliceRows(1L, 3L, n34)
    return(morloc_put_value(n35, mlc_schema_table[[5]]))
}
m4760 <- function(s32)
{
    n32 <- morloc_get_value(s32, mlc_schema_table[[6]])
    n33 <- morloc_asCol("x", n32)
    return(morloc_put_value(n33, mlc_schema_table[[5]]))
}
m4769 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n35 <- morloc_sliceRows(1L, 4L, n34)
    return(morloc_put_value(n35, mlc_schema_table[[5]]))
}
m4774 <- function(s32)
{
    n32 <- morloc_get_value(s32, mlc_schema_table[[6]])
    n33 <- morloc_asCol("x", n32)
    return(morloc_put_value(n33, mlc_schema_table[[5]]))
}
m4783 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n35 <- morloc_sliceRows(2L, 3L, n34)
    return(morloc_put_value(n35, mlc_schema_table[[5]]))
}
m4788 <- function(s30)
{
    n30 <- morloc_get_value(s30, mlc_schema_table[[21]])
    n31 <- morloc_asCol("x", n30)
    return(morloc_put_value(n31, mlc_schema_table[[5]]))
}
m4799 <- function(n34)
{
    n35 <- morloc_sliceRows(3L, 4L, n34)
    return(n35)
}

m4797 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n36 <- morloc_nrow(m4799(n34))
    return(morloc_put_value(n36, mlc_schema_table[[10]]))
}
m4810 <- function(n34)
{
    n35 <- morloc_sliceRows(5L, 9L, n34)
    return(n35)
}

m4808 <- function(s34)
{
    n34 <- morloc_get_value(s34, mlc_schema_table[[5]])
    n36 <- morloc_nrow(m4810(n34))
    return(morloc_put_value(n36, mlc_schema_table[[10]]))
}
m4923 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m4921 <- function()
{
    n3 <- morloc_names(m4923())
    return(morloc_put_value(n3, mlc_schema_table[[23]]))
}
m4961 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m4957 <- function()
{
    n2 <- c("0", "1", "2")
    n5 <- morloc_setCol("y", n2, m4961())
    return(n5)
}

m4933 <- function()
{
    n1 <- c(0.0, 0.5, 1.0)
    n6 <- morloc_setCol("z", n1, m4957())
    return(n6)
}

m4931 <- function()
{
    n7 <- morloc_names(m4933())
    return(morloc_put_value(n7, mlc_schema_table[[23]]))
}
m4995 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m4999 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(morloc_put_value(n2, mlc_schema_table[[5]]))
}
m5007 <- function()
{
    n1 <- c("0", "1", "2")
    n2 <- morloc_asCol("y", n1)
    return(morloc_put_value(n2, mlc_schema_table[[13]]))
}
m5014 <- function()
{
    n1 <- c("0", "1", "2")
    n2 <- morloc_asCol("y", n1)
    return(morloc_put_value(n2, mlc_schema_table[[13]]))
}
m5024 <- function()
{
    n1 <- c(0L, 1L, 2L)
    n2 <- morloc_asCol("x", n1)
    return(n2)
}

m5022 <- function()
{
    n3 <- morloc_nrow(m5024())
    return(morloc_put_value(n3, mlc_schema_table[[10]]))
}
m5092 <- function()
{
    n3 <- c(0L, 1L, 2L)
    n4 <- morloc_asCol("x", n3)
    return(n4)
}

m5088 <- function()
{
    n2 <- c("0", "1", "2")
    n5 <- morloc_setCol("y", n2, m5092())
    return(n5)
}

m5033 <- function()
{
    n1 <- c(0.0, 0.5, 1.0)
    n6 <- morloc_setCol("z", n1, m5088())
    return(n6)
}

m5031 <- function()
{
    n7 <- morloc_ncol(m5033())
    return(morloc_put_value(n7, mlc_schema_table[[10]]))
}
# AUTO include manifolds end

# AUTO include dispatch start
.dispatch <- list()
.dispatch[[1L]] <- m1
.dispatch[[2739L]] <- m2739
.dispatch[[2875L]] <- m2875
.dispatch[[2878L]] <- m2878
.dispatch[[2881L]] <- m2881
.dispatch[[2885L]] <- m2885
.dispatch[[2893L]] <- m2893
.dispatch[[2898L]] <- m2898
.dispatch[[2907L]] <- m2907
.dispatch[[2919L]] <- m2919
.dispatch[[2926L]] <- m2926
.dispatch[[2940L]] <- m2940
.dispatch[[2985L]] <- m2985
.dispatch[[2992L]] <- m2992
.dispatch[[3001L]] <- m3001
.dispatch[[3017L]] <- m3017
.dispatch[[3031L]] <- m3031
.dispatch[[3045L]] <- m3045
.dispatch[[3053L]] <- m3053
.dispatch[[3187L]] <- m3187
.dispatch[[3194L]] <- m3194
.dispatch[[3201L]] <- m3201
.dispatch[[3268L]] <- m3268
.dispatch[[3273L]] <- m3273
.dispatch[[3279L]] <- m3279
.dispatch[[3375L]] <- m3375
.dispatch[[3382L]] <- m3382
.dispatch[[3390L]] <- m3390
.dispatch[[3397L]] <- m3397
.dispatch[[3403L]] <- m3403
.dispatch[[3409L]] <- m3409
.dispatch[[3541L]] <- m3541
.dispatch[[3546L]] <- m3546
.dispatch[[3554L]] <- m3554
.dispatch[[3560L]] <- m3560
.dispatch[[3566L]] <- m3566
.dispatch[[3573L]] <- m3573
.dispatch[[3700L]] <- m3700
.dispatch[[3704L]] <- m3704
.dispatch[[3709L]] <- m3709
.dispatch[[3721L]] <- m3721
.dispatch[[3728L]] <- m3728
.dispatch[[3736L]] <- m3736
.dispatch[[3743L]] <- m3743
.dispatch[[3901L]] <- m3901
.dispatch[[3913L]] <- m3913
.dispatch[[3921L]] <- m3921
.dispatch[[4036L]] <- m4036
.dispatch[[4045L]] <- m4045
.dispatch[[4053L]] <- m4053
.dispatch[[4063L]] <- m4063
.dispatch[[4071L]] <- m4071
.dispatch[[4080L]] <- m4080
.dispatch[[4087L]] <- m4087
.dispatch[[4093L]] <- m4093
.dispatch[[4188L]] <- m4188
.dispatch[[4192L]] <- m4192
.dispatch[[4196L]] <- m4196
.dispatch[[4202L]] <- m4202
.dispatch[[4206L]] <- m4206
.dispatch[[4210L]] <- m4210
.dispatch[[4214L]] <- m4214
.dispatch[[4221L]] <- m4221
.dispatch[[4226L]] <- m4226
.dispatch[[4234L]] <- m4234
.dispatch[[4237L]] <- m4237
.dispatch[[4244L]] <- m4244
.dispatch[[4333L]] <- m4333
.dispatch[[4339L]] <- m4339
.dispatch[[4347L]] <- m4347
.dispatch[[4353L]] <- m4353
.dispatch[[4362L]] <- m4362
.dispatch[[4368L]] <- m4368
.dispatch[[4375L]] <- m4375
.dispatch[[4381L]] <- m4381
.dispatch[[4488L]] <- m4488
.dispatch[[4494L]] <- m4494
.dispatch[[4502L]] <- m4502
.dispatch[[4516L]] <- m4516
.dispatch[[4522L]] <- m4522
.dispatch[[4529L]] <- m4529
.dispatch[[4622L]] <- m4622
.dispatch[[4624L]] <- m4624
.dispatch[[4626L]] <- m4626
.dispatch[[4628L]] <- m4628
.dispatch[[4631L]] <- m4631
.dispatch[[4634L]] <- m4634
.dispatch[[4638L]] <- m4638
.dispatch[[4646L]] <- m4646
.dispatch[[4659L]] <- m4659
.dispatch[[4673L]] <- m4673
.dispatch[[4678L]] <- m4678
.dispatch[[4687L]] <- m4687
.dispatch[[4692L]] <- m4692
.dispatch[[4701L]] <- m4701
.dispatch[[4706L]] <- m4706
.dispatch[[4715L]] <- m4715
.dispatch[[4728L]] <- m4728
.dispatch[[4741L]] <- m4741
.dispatch[[4746L]] <- m4746
.dispatch[[4755L]] <- m4755
.dispatch[[4760L]] <- m4760
.dispatch[[4769L]] <- m4769
.dispatch[[4774L]] <- m4774
.dispatch[[4783L]] <- m4783
.dispatch[[4788L]] <- m4788
.dispatch[[4797L]] <- m4797
.dispatch[[4808L]] <- m4808
.dispatch[[4921L]] <- m4921
.dispatch[[4931L]] <- m4931
.dispatch[[4995L]] <- m4995
.dispatch[[4999L]] <- m4999
.dispatch[[5007L]] <- m5007
.dispatch[[5014L]] <- m5014
.dispatch[[5022L]] <- m5022
.dispatch[[5031L]] <- m5031
.remote_dispatch <- list()
# AUTO include dispatch end

worker_loop <- function(pipe_fd) {
  morloc_worker_loop_c(pipe_fd, .dispatch, .remote_dispatch)
}

main <- function(socket_path, tmpdir, shm_basename) {
  # Force line-buffered stdout/stderr so output from user functions is not lost
  # when the nexus kills the pool process group.
  morloc_set_line_buffered()
  morloc_install_sigterm_handler()

  daemon <- morloc_start_daemon(socket_path, tmpdir, shm_basename, 0xffff)
  n_workers <- 1L

  # Shared job queue: dispatcher writes fds to fd[1], workers read from fd[2].
  # Only idle workers (blocked in recvmsg) pick up jobs, preventing the
  # round-robin deadlock where a callback gets dispatched to a busy worker.
  job_queue <- morloc_socketpair()

  # Shared counter for dynamic worker spawning
  busy_counter <- morloc_shared_counter_create()
  wakeup <- morloc_pipe()  # c(read_fd, write_fd)

  # Set globals so the monkey-patched morloc_foreign_call can use them.
  # Forked children inherit these values.
  .busy_counter <<- busy_counter
  .wakeup_fd <<- wakeup[2L]
  .n_workers_total <<- n_workers

  pids <- integer(n_workers)
  for (i in seq_len(n_workers)) {
    pid <- morloc_fork()
    if (pid == 0L) {
      morloc_detach_daemon(daemon)
      morloc_close_socket(job_queue[1L])  # child doesn't write
      morloc_close_fd(wakeup[1L])         # child doesn't read wakeup pipe
      worker_loop(job_queue[2L])
      morloc_exit(0L)
    }
    pids[i] <- pid
  }
  # Keep job_queue[2L] open so dynamically spawned children can use it

  on.exit({
    tryCatch(morloc_close_socket(job_queue[1L]), error = function(e) NULL)
    tryCatch(morloc_close_socket(job_queue[2L]), error = function(e) NULL)
    tryCatch(morloc_close_fd(wakeup[1L]), error = function(e) NULL)
    tryCatch(morloc_close_fd(wakeup[2L]), error = function(e) NULL)
    for (pid in pids) {
      if (pid > 0L) {
        tryCatch(morloc_kill(pid, 9L), error = function(e) NULL)
        tryCatch(morloc_waitpid_blocking(pid), error = function(e) NULL)
      }
    }
  })

  # Dispatch loop - idle workers pull from shared queue.
  # After each dispatch cycle, check if all workers are busy and spawn more.
  while (!morloc_is_shutting_down()) {
    client_fd <- morloc_wait_for_client(daemon)
    if (client_fd > 0L) {
      tryCatch({
        morloc_send_fd(job_queue[1L], client_fd)
      }, error = function(e) {
        cat(paste("Failed to dispatch job:", e$message, "\n"), file = stderr())
      }, finally = {
        morloc_close_socket(client_fd)
      })
    }

    # Dynamic worker spawning: if all workers are blocked in foreign_call,
    # spawn a new one so incoming callbacks can still be served.
    current_busy <- morloc_shared_counter_read(busy_counter)
    if (current_busy >= n_workers) {
      pid <- morloc_fork()
      if (pid == 0L) {
        morloc_detach_daemon(daemon)
        morloc_close_socket(job_queue[1L])
        morloc_close_fd(wakeup[1L])
        worker_loop(job_queue[2L])
        morloc_exit(0L)
      }
      pids <- c(pids, pid)
      n_workers <- n_workers + 1L
      .n_workers_total <<- n_workers
    }
  }
}

args <- commandArgs(trailingOnly = TRUE)

# Health check: confirm sources loaded and print version
if (length(args) == 1 && args[1] == "--health") {
  cat('{"status":"ok","version":"0.82.0"}\n')
  quit(status = 0)
}

if (length(args) != 3) {
  cat("Usage: Rscript pool.R <socket_path> <tmpdir> <shm_basename>\n", file=stderr())
  quit(status = 1)
}

socket_path <- args[1]
tmpdir <- args[2]
shm_basename <- args[3]

global_state$tmpdir <- tmpdir

tryCatch(
  {
    main(socket_path, tmpdir, shm_basename)
  },  error = function(e) {
      stop(paste("Pool failed:", e$message))
  })

# Use _exit to avoid R cleanup which triggers heap corruption on glibc >= 2.39
# (R's finalizers attempt to free objects in SHM-related C extensions)
morloc_exit(0L)
