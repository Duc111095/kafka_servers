from datetime import datetime
from dataclasses import dataclass


@dataclass
class Tbmt:
    stt: int
    stt_rec: str
    so_ct: str
    ngay_lct: datetime
    ma_kh: str
    ma_bp: str
    ma_nvbh: str
    ma_nvbh_th: str
    ten_thau: str
    loai_hinh_thau: str
    fdate1: datetime
    fdate2: datetime
    ngay_dong_thau: datetime
    t_tien_nt: float
    ten_kh: str
    ten_bp: str

    def to_table(self) -> str:
        msg = f'| {self.so_ct} | {self.ma_bp} | {self.ma_nvbh_th} | {self.ma_kh} | {self.ten_kh} | {self.ten_thau} | {self.loai_hinh_thau} | {self.ngay_dong_thau} |'
        return msg

    def to_task(self) -> str:
        task = f'Task {self.stt}: {self.ma_bp} - {self.ma_nvbh_th} - {self.so_ct} - {self.ten_thau} - {self.ten_kh} - Ngày đóng thầu: {datetime.strftime(self.ngay_dong_thau,"%d-%m-%Y")}\n'
        return task
