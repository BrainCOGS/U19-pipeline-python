"""Explicit source identity selection from a read-only lab.Location snapshot."""

from PyQt6.QtWidgets import QComboBox, QDialog, QDialogButtonBox, QFormLayout, QLabel

from .rigs import MODALITIES, identity, matching_locations


class RigDialog(QDialog):
    def __init__(self, parent, rows, machine):
        super().__init__(parent)
        self.setWindowTitle("Select this PC's rig")
        self.rows = rows
        self.current = identity(machine) or {}
        self.setMinimumWidth(560)
        form = QFormLayout(self)
        self.role = QComboBox()
        self.role.addItem("Recording computer", "recording")
        self.role.addItem("Acquisition rig", "rig")
        self.acquisition = QComboBox()
        self.location = QComboBox()
        self.note = QLabel(
            "Choose the source machine's database location. This selection is saved on this PC and used for future copies and registration."
        )
        self.note.setWordWrap(True)
        self.buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save
            | QDialogButtonBox.StandardButton.Cancel
        )
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        form.addRow("Rig type", self.role)
        form.addRow("Acquisition type", self.acquisition)
        form.addRow("lab.Location", self.location)
        form.addRow(self.note)
        form.addRow(self.buttons)
        self.role.currentIndexChanged.connect(self.fill_acquisition)
        self.acquisition.currentIndexChanged.connect(self.fill_locations)
        self.location.currentIndexChanged.connect(self.update_save)
        self.role.setCurrentIndex(
            max(0, self.role.findData(self.current.get("system_type", "recording")))
        )
        self.fill_acquisition()
        if self.current:
            index = self.acquisition.findData(self.current.get("acquisition_type"))
            if index >= 0:
                self.acquisition.setCurrentIndex(index)
            for index in range(self.location.count()):
                row = self.location.itemData(index)
                if row and row["location"] == self.current.get("location"):
                    self.location.setCurrentIndex(index)

    def fill_acquisition(self):
        self.acquisition.blockSignals(True)
        self.acquisition.clear()
        self.acquisition.addItem("All supported acquisition types", None)
        for kind in sorted(
            {
                row["acquisition_type"]
                for row in self.rows
                if row.get("system_type") == self.role.currentData()
                and row.get("acquisition_type") in MODALITIES
            }
        ):
            self.acquisition.addItem(kind, kind)
        self.acquisition.blockSignals(False)
        self.fill_locations()

    def fill_locations(self):
        self.location.clear()
        self.location.addItem("Choose a location…", None)
        for row in matching_locations(
            self.rows, self.role.currentData(), self.acquisition.currentData()
        ):
            self.location.addItem(
                f"{row['location']}  ·  {row['acquisition_type']}", row
            )
        self.update_save()

    def update_save(self):
        self.buttons.button(QDialogButtonBox.StandardButton.Save).setEnabled(
            bool(self.location.currentData())
        )
