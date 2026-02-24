from hotglue_singer_sdk.tap_base import InvalidCredentialsError


class MissingPermissionsError(InvalidCredentialsError):
    """Exception raised for missing permission."""
    pass