"""Axum benchmark capabilities; GCRA remains semantically distinct."""
VARIANTS = {
    'baseline': dict(algorithm='none', backends=['memory','redis'], zero=True, oracle=False),
    'head': dict(algorithm='exact sliding log', backends=['memory','redis'], zero=True, oracle=True),
    'governor': dict(algorithm='GCRA', backends=['memory'], zero=False, oracle=False),
    'ratelimit': dict(algorithm='token bucket (keyed Axum adapter)', backends=['memory'], zero=False, oracle=False),
}


def supports(variant, backend, mode):
    config = VARIANTS[variant]
    return backend in config['backends'] and (mode != 'deny' or config['zero'])
