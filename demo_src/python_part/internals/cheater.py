import pathlib


def cheating_with_secret():
    my_file = pathlib.Path("/tmp/z_secret")
    print('checking secret file\n')
    if my_file.exists():
        raise Exception("magic_file")