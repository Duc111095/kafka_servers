import datetime
from entity.tbmt import Tbmt


def table_to_send(tbmts: list[Tbmt]) -> str:
    msg = f'''
    Thông tin Thầu - Ngày {datetime.now().date()}:

    | Số TBMT | BP | Mã NV thực hiện | Mã KH | Tên KH | Tên gói thầu | Loại hình thầu | Ngày đóng thầu |
    |-------|-------|:----------:|:-----------:|---------------------|-------------------|---------|:------:|
    '''
    for t in tbmts:
        tb: Tbmt = Tbmt(*t)
        msg += tb.to_table()
    return msg


def task_to_send(tbmts: list[Tbmt]) -> str:
    msg_task = f'''
    /todo Thông báo thầu đến hạn - Ngày {datetime.strftime(datetime.now().date(), "%d-%m-%Y")}
    '''
    for t in tbmts:
        tb: Tbmt = Tbmt(*t)
        msg_task += tb.to_task()
    return msg_task