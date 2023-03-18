a = [("1", "a"), (None, "b")]
print(sorted(a, key=lambda x: int(x[0]) if x[0] else -1))
