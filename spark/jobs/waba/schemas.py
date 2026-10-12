from dataclasses import dataclass


@dataclass
class Dataset:
    name: str
    id_col: str
    columns: list
    required: list
    folder: str = None
    prefix: str = None
    ts_col: str = None
    topic: str = None
    silver_topic: str = None
    referential: bool = False
    currency_check: bool = True

    @property
    def column_names(self):
        return [c for c, _ in self.columns]


BANK = Dataset(
    name="bank_transactions", id_col="transaction_id", folder="bank_transactions", prefix="bank_txn",
    ts_col="timestamp", topic="raw-bank-transactions", silver_topic="silver-bank-transactions",
    columns=[
        ("transaction_id", "string"), ("timestamp", "timestamp"), ("account_id", "string"),
        ("beneficiary_account", "string"), ("branch_id", "string"), ("country_code", "string"),
        ("transaction_type", "string"), ("amount", "double"), ("currency", "string"), ("channel", "string"),
        ("transaction_status", "string"), ("fee_amount", "double"), ("entity_type", "string"),
    ],
    required=["transaction_id", "timestamp", "account_id", "branch_id", "country_code", "amount", "currency",
              "transaction_type"],
)

INSURANCE = Dataset(
    name="insurance_operations", id_col="operation_id", folder="insurance_operations", prefix="insurance_ops",
    ts_col="timestamp", topic="raw-insurance-operations", silver_topic="silver-insurance-operations",
    columns=[
        ("operation_id", "string"), ("timestamp", "timestamp"), ("customer_id", "string"), ("account_id", "string"),
        ("country_code", "string"), ("operation_type", "string"), ("product_line", "string"), ("amount", "double"),
        ("currency", "string"), ("claim_status", "string"), ("processing_days", "int"), ("entity_type", "string"),
    ],
    required=["operation_id", "timestamp", "customer_id", "account_id", "country_code", "operation_type",
              "amount", "currency"],
)

MOBILE = Dataset(
    name="mobile_money_payments", id_col="payment_id", folder="mobile_money_payments", prefix="mobile_money",
    ts_col="timestamp", topic="raw-mobile-money-payments", silver_topic="silver-mobile-money",
    columns=[
        ("payment_id", "string"), ("timestamp", "timestamp"), ("sender_id", "string"), ("receiver_id", "string"),
        ("sender_country", "string"), ("receiver_country", "string"), ("country_code", "string"),
        ("amount", "double"), ("currency", "string"), ("payment_type", "string"), ("operator", "string"),
        ("status", "string"), ("fee_amount", "double"), ("entity_type", "string"),
    ],
    required=["payment_id", "timestamp", "sender_id", "receiver_id", "country_code", "amount", "currency"],
)

LOANS = Dataset(
    name="loan_repayments", id_col="repayment_id", folder="loan_repayments", prefix="loan_repayments",
    ts_col="timestamp", topic="raw-loan-repayments",
    columns=[
        ("repayment_id", "string"), ("timestamp", "timestamp"), ("loan_account_id", "string"),
        ("customer_id", "string"), ("country_code", "string"), ("amount_due", "double"), ("amount_paid", "double"),
        ("currency", "string"), ("due_date", "date"), ("payment_date", "date"), ("days_overdue", "int"),
        ("loan_type", "string"), ("repayment_status", "string"), ("entity_type", "string"),
    ],
    required=["repayment_id", "timestamp", "loan_account_id", "customer_id", "country_code", "amount_due",
              "currency", "repayment_status"],
)

CUSTOMERS = Dataset(
    name="customers", id_col="customer_id", referential=True, currency_check=False,
    columns=[
        ("customer_id", "string"), ("country_code", "string"), ("entity_type", "string"), ("segment", "string"),
        ("kyc_level", "string"), ("onboarding_date", "date"), ("region", "string"), ("is_active", "boolean"),
    ],
    required=["customer_id", "country_code"],
)

ACCOUNTS = Dataset(
    name="accounts", id_col="account_id", referential=True,
    columns=[
        ("account_id", "string"), ("customer_id", "string"), ("country_code", "string"),
        ("account_type", "string"), ("currency", "string"), ("balance", "double"), ("credit_limit", "double"),
        ("opened_date", "date"), ("status", "string"), ("entity_type", "string"),
    ],
    required=["account_id", "customer_id", "country_code", "account_type", "currency"],
)

BRANCHES = Dataset(
    name="branches", id_col="branch_id", referential=True, currency_check=False,
    columns=[
        ("branch_id", "string"), ("country_code", "string"), ("entity_type", "string"), ("city", "string"),
        ("region", "string"), ("branch_type", "string"), ("is_active", "boolean"),
    ],
    required=["branch_id", "country_code"],
)

PRODUCTS = Dataset(
    name="products", id_col="product_id", referential=True, currency_check=False,
    columns=[
        ("product_id", "string"), ("product_code", "string"), ("product_name", "string"),
        ("product_family", "string"), ("interest_rate", "double"), ("country_code", "string"),
        ("entity_type", "string"),
    ],
    required=["product_id", "product_code", "country_code"],
)

TRANSACTIONAL = [BANK, INSURANCE, MOBILE, LOANS]
REFERENTIALS = [CUSTOMERS, ACCOUNTS, BRANCHES, PRODUCTS]
ALL = REFERENTIALS + TRANSACTIONAL
BY_NAME = {d.name: d for d in ALL}
BY_TOPIC = {d.topic: d for d in TRANSACTIONAL}
