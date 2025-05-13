import string, random


def random_name() -> str:
    return "".join(random.choice(string.ascii_letters) for _ in range(6))
