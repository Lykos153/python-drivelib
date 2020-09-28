from googleapiclient.errors import HttpError
import json

class GoogleDriveAPIError(HttpError):
    @classmethod
    def from_reply(cls, resp, content, uri=None):
        err = HttpError(resp, content, uri)
        return cls.from_http_error(err)

    @classmethod
    def from_http_error(cls, err: HttpError):
        error_mapping = {
            400: BadRequestError,
            401: InvalidCredentialsError,
            404: FileNotFoundError,
            429: TooManyRequestsError,
            500: BackendError
        }
        if err.resp.status in error_mapping:
            return error_mapping[err.resp.status](*err.args)
        elif err.resp.status == 403:
            return UsageLimitExceededError.from_http_error(
                                            cls(*err.args)
                                            )
        else:
            return cls(*err.args)
        

class BadRequestError(GoogleDriveAPIError):
    pass



class InvalidCredentialsError(GoogleDriveAPIError):
    pass



class UsageLimitExceededError(GoogleDriveAPIError):
    @classmethod
    def from_http_error(cls, err: GoogleDriveAPIError):
        reason_mapping = {
            'dailyLimitExceeded': DailyLimitExceededError,
            'numChildrenInNonRootLimitExceeded': NumberOfChildrenExceededError,
            'userRateLimitExceeded': UserRateLimitExceededError,
            'rateLimitExceeded': RateLimitExceededError,
            'sharingRateLimitExceeded': SharingRateLimitExceededError,
            'appNotAuthorizedToFile': NotAuthorizedError,
            'insufficientFilePermissions': InsufficientFilePermissionsError,
        }

        try:
            body = json.loads(err.content.decode("utf-8"))
        except:
            return err
        if len(body['error']['errors']) == 1:
            reason = body['error']['errors'][0]['reason']
        else:
            return err
        
        if reason in reason_mapping:
            return reason_mapping[reason](*err.args)
        else:
            return err

class DailyLimitExceededError(UsageLimitExceededError):
    pass

class NumberOfChildrenExceededError(UsageLimitExceededError):
    pass

class UserRateLimitExceededError(UsageLimitExceededError):
    pass

class RateLimitExceededError(UsageLimitExceededError):
    pass

class SharingRateLimitExceededError(UsageLimitExceededError):
    pass

class NotAuthorizedError(GoogleDriveAPIError):
    pass

class AppNotAuthorizedError(NotAuthorizedError):
    pass

class InsufficientFilePermissionsError(NotAuthorizedError):
    pass



class TooManyRequestsError(GoogleDriveAPIError):
    pass



class BackendError(GoogleDriveAPIError):
    pass
