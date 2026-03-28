from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime
from enum import Enum


class SignalType(str, Enum):
    new_license = "new_license"
    activation = "activation"
    escrowed = "escrowed"
    reactivation = "reactivation"
    location_change = "location_change"
    removed = "removed"


SIGNAL_LABELS = {
    SignalType.new_license: "New License",
    SignalType.activation: "Activation: Ready to Buy",
    SignalType.escrowed: "Broker Signal: On the Market",
    SignalType.reactivation: "Market Re-entry",
    SignalType.location_change: "Location Change",
    SignalType.removed: "License Removed",
}


class LicenseChange(BaseModel):
    license_number: str
    dba_name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    state: str = "MI"
    license_type: Optional[str] = None
    previous_status: Optional[str] = None
    current_status: Optional[str] = None
    signal_type: SignalType
    signal_label: str
    detected_date: date

    class Config:
        use_enum_values = True


class DeltaSummary(BaseModel):
    new_licenses: int = 0
    activations: int = 0
    escrowed: int = 0
    reactivations: int = 0
    location_changes: int = 0
    removed: int = 0
    total: int = 0


class DeltaResponse(BaseModel):
    week_ending: date
    generated_at: datetime
    source: str = "Michigan LARA - Liquor Control Commission"
    source_url: str = "https://www.michigan.gov/lara/bureau-list/lcc/licensing-list"
    total_changes: int
    summary: DeltaSummary
    changes: List[LicenseChange]


class AvailableWeek(BaseModel):
    week_ending: date
    total_changes: int
    generated_at: datetime


class WeeksResponse(BaseModel):
    available_weeks: List[AvailableWeek]
    count: int


class HealthResponse(BaseModel):
    status: str
    latest_week: Optional[date]
    total_records_stored: int
    version: str = "1.0.0"
