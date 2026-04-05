from aiogram.fsm.state import State, StatesGroup


class UserPromoStates(StatesGroup):
    waiting_for_promo_code = State()


class UserPartnerStates(StatesGroup):
    waiting_for_custom_slug = State()


class UserPartnerWithdrawalStates(StatesGroup):
    waiting_for_method = State()
    waiting_for_sbp_phone = State()
    waiting_for_sbp_bank = State()
    waiting_for_sbp_fio = State()
    waiting_for_usdt_wallet = State()
    waiting_for_amount = State()
    waiting_for_confirmation = State()
