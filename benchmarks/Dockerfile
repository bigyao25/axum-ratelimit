# CI compiles the benchmark binary using the committed Cargo.lock.
# A matching Ubuntu base avoids mismatched glibc between runner and image.
FROM ubuntu:24.04
COPY benchmarks/docker-bin/benchmark-server /usr/local/bin/benchmark-server
USER 65534:65534
ENV BENCH_BIND=0.0.0.0:8080
ENTRYPOINT ["/usr/local/bin/benchmark-server"]
