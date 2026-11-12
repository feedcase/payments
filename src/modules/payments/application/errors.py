class ApplicationError(Exception):
    pass


class RateLimited(ApplicationError):
    pass


class IdempotencyMismatch(ApplicationError):
    pass


class NotFound(ApplicationError):
    pass


class InvalidState(ApplicationError):
    pass


class DuplicatePayment(ApplicationError):
    pass


class ConcurrencyConflict(ApplicationError):
    pass


class DuplicateProviderPaymentId(ApplicationError):
    """Провайдер вернул id, который уже принадлежит другой попытке."""
