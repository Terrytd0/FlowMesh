from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RiskBand(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RISK_BAND_UNSPECIFIED: _ClassVar[RiskBand]
    RISK_BAND_LOW: _ClassVar[RiskBand]
    RISK_BAND_AMBIGUOUS: _ClassVar[RiskBand]
    RISK_BAND_HIGH: _ClassVar[RiskBand]
RISK_BAND_UNSPECIFIED: RiskBand
RISK_BAND_LOW: RiskBand
RISK_BAND_AMBIGUOUS: RiskBand
RISK_BAND_HIGH: RiskBand

class LineItem(_message.Message):
    __slots__ = ("sku", "quantity")
    SKU_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    sku: str
    quantity: int
    def __init__(self, sku: _Optional[str] = ..., quantity: _Optional[int] = ...) -> None: ...

class PaymentContext(_message.Message):
    __slots__ = ("card_bin", "card_last4", "card_country", "billing_country", "shipping_country", "ip_country", "coupon_code", "is_gift_card")
    CARD_BIN_FIELD_NUMBER: _ClassVar[int]
    CARD_LAST4_FIELD_NUMBER: _ClassVar[int]
    CARD_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    BILLING_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    IP_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    COUPON_CODE_FIELD_NUMBER: _ClassVar[int]
    IS_GIFT_CARD_FIELD_NUMBER: _ClassVar[int]
    card_bin: str
    card_last4: str
    card_country: str
    billing_country: str
    shipping_country: str
    ip_country: str
    coupon_code: str
    is_gift_card: bool
    def __init__(self, card_bin: _Optional[str] = ..., card_last4: _Optional[str] = ..., card_country: _Optional[str] = ..., billing_country: _Optional[str] = ..., shipping_country: _Optional[str] = ..., ip_country: _Optional[str] = ..., coupon_code: _Optional[str] = ..., is_gift_card: _Optional[bool] = ...) -> None: ...

class FraudRequest(_message.Message):
    __slots__ = ("order_id", "customer_id", "items", "payment", "total_cents", "received_at", "correlation_id", "require_rationale")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CENTS_FIELD_NUMBER: _ClassVar[int]
    RECEIVED_AT_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUIRE_RATIONALE_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    customer_id: str
    items: _containers.RepeatedCompositeFieldContainer[LineItem]
    payment: PaymentContext
    total_cents: int
    received_at: str
    correlation_id: str
    require_rationale: bool
    def __init__(self, order_id: _Optional[str] = ..., customer_id: _Optional[str] = ..., items: _Optional[_Iterable[_Union[LineItem, _Mapping]]] = ..., payment: _Optional[_Union[PaymentContext, _Mapping]] = ..., total_cents: _Optional[int] = ..., received_at: _Optional[str] = ..., correlation_id: _Optional[str] = ..., require_rationale: _Optional[bool] = ...) -> None: ...

class ScoreContribution(_message.Message):
    __slots__ = ("feature", "value", "weight", "contribution")
    FEATURE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_FIELD_NUMBER: _ClassVar[int]
    CONTRIBUTION_FIELD_NUMBER: _ClassVar[int]
    feature: str
    value: float
    weight: float
    contribution: float
    def __init__(self, feature: _Optional[str] = ..., value: _Optional[float] = ..., weight: _Optional[float] = ..., contribution: _Optional[float] = ...) -> None: ...

class FraudResponse(_message.Message):
    __slots__ = ("order_id", "score", "band", "reasons", "rationale", "model_version", "latency_ms", "degraded", "llm_used", "contributions", "features")
    class FeaturesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    BAND_FIELD_NUMBER: _ClassVar[int]
    REASONS_FIELD_NUMBER: _ClassVar[int]
    RATIONALE_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_FIELD_NUMBER: _ClassVar[int]
    LLM_USED_FIELD_NUMBER: _ClassVar[int]
    CONTRIBUTIONS_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    score: float
    band: RiskBand
    reasons: _containers.RepeatedScalarFieldContainer[str]
    rationale: str
    model_version: str
    latency_ms: float
    degraded: bool
    llm_used: bool
    contributions: _containers.RepeatedCompositeFieldContainer[ScoreContribution]
    features: _containers.ScalarMap[str, float]
    def __init__(self, order_id: _Optional[str] = ..., score: _Optional[float] = ..., band: _Optional[_Union[RiskBand, str]] = ..., reasons: _Optional[_Iterable[str]] = ..., rationale: _Optional[str] = ..., model_version: _Optional[str] = ..., latency_ms: _Optional[float] = ..., degraded: _Optional[bool] = ..., llm_used: _Optional[bool] = ..., contributions: _Optional[_Iterable[_Union[ScoreContribution, _Mapping]]] = ..., features: _Optional[_Mapping[str, float]] = ...) -> None: ...

class HealthRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HealthResponse(_message.Message):
    __slots__ = ("ready", "model_version", "orders_scored", "tracked_customers")
    READY_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    ORDERS_SCORED_FIELD_NUMBER: _ClassVar[int]
    TRACKED_CUSTOMERS_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    model_version: str
    orders_scored: int
    tracked_customers: int
    def __init__(self, ready: _Optional[bool] = ..., model_version: _Optional[str] = ..., orders_scored: _Optional[int] = ..., tracked_customers: _Optional[int] = ...) -> None: ...
