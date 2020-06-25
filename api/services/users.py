import bcrypt


def convert_pwd(password: bytes) -> bytes:
    """
    Function for convert password to hash with salt
    """
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password=password, salt=salt)

    return hashed
