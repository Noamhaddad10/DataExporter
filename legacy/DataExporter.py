import datetime
import json
import queue
import sys
import time
import os

from logger_jumping import PlaybackLoggerFile
import DataExporterUi
from timeline import _Timeline

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtGui import QKeySequence
from PyQt5.QtWidgets import QApplication, QListWidgetItem, QFileDialog, QLabel, QShortcut
from PyQt5.QtCore import QThread, QTimer

from scipy.interpolate import interp1d


class FileLoader(QThread):
    """
    Pop up for choosing, then opening, the desired logger file
    """

    def __init__(self, filepath: str, exercise_id: int, data_pipe_sender: queue.SimpleQueue):
        """
        :param filepath:            str                 : The absolute path to the file
        :param exercise_id:         int                 :
        :param data_pipe_sender:    queue.SimpleQueue   : SimpleQueue through which the PlaybackLoggerFile instance is
                                                          returned
        """
        super().__init__()
        self._filepath = filepath
        self._exercise_id = exercise_id
        self._data_pipe = data_pipe_sender

    def run(self):
        plg = PlaybackLoggerFile(self._filepath, self._exercise_id)
        self._data_pipe.put(plg)


class DataExporter(QtWidgets.QMainWindow, DataExporterUi.Ui_MainWindow):
    """
    The class derived from PyQT which is the main window for the DataExporter - Player
    Through this window all playback options are managed by the user. They control speed, exercise id, and which parts
    of the logger are played to the network.
    """

    def __init__(self, parent=None):
        super(DataExporter, self).__init__(parent)
        self.setupUi(self)

        self.logger_file_name = ""
        self.actionOpenFile.triggered.connect(self._choose_file)
        self._base_window_title = self.windowTitle()

        self.length_logger_file = 0
        # maps the timeline position to the time in the logger file
        self.position_mapper = interp1d([0, 100], [0, self.length_logger_file])
        # Map times to a timeline position
        self.time_mapper = interp1d([0, self.length_logger_file], [0, 100])
        self.timeline_width = 100

        self.play_back_loggerfile: PlaybackLoggerFile = None
        self._data_channel = queue.SimpleQueue()  # Receives the instantiated PlaybackLoggerFile from the FileLoader

        self._starting_worldtime = 0
        self._starting_packettime = 0
        self._approximate_current_packettime = 0
        self._change_playback_position_timer = QTimer()
        self._change_playback_position_timer.timeout.connect(self._timer_timeout)

        self._display_time(0)
        self._setup_shortcuts()

        self.playback_speed = 1
        self.exercise_id = int(self.spinBoxExerciseId.text())
        self._load_history()

        self._ui_connected = False

        self.buttonPlay.setDisabled(True)
        self.buttonStop.setDisabled(True)
        self.buttonPause.setDisabled(True)
        self.buttonJumpStart.setDisabled(True)
        self.buttonIncreaseSpeed.setDisabled(True)
        self.buttonDecreaseSpeed.setDisabled(True)

        self.buttonConnect.setDisabled(True)
        self.buttonDisconnect.setDisabled(True)
        self.spinBoxExerciseId.setDisabled(True)

    def _load_history(self) -> None:
        """
        Checks if the session file, and folder exist. If they do not exist, then it creates them.
        Then reads the session file, and loads the data into relevant places.
        :return: None
        """
        if "DataPlayer" not in os.listdir("C:/"):
            os.mkdir("C:/DataPlayer")
            with open("C:/DataPlayer/session.json", 'w') as f:
                json.dump({"exercise_id": 21}, f)

        with open("C:/DataPlayer/session.json", 'r') as f:
            last_session = json.load(f)
            self.exercise_id = last_session["exercise_id"]

        self.spinBoxExerciseId.setValue(self.exercise_id)

    def _write_session_history(self) -> None:
        """
        Writes the session data to the session file when closed.
        :return: None
        """
        session_data = {
            "exercise_id": self.exercise_id
        }

        with open("C:/DataPlayer/session.json", 'w') as f:
            json.dump(session_data, f)

    def closeEvent(self, a0: QtGui.QCloseEvent) -> None:
        """
        Called automatically when the DataPlayer is closed, writes the session history, and finishes closing the wiundow
        :param a0: QtGui.QCloseEvent
        :return: None
        """
        self._write_session_history()
        a0.accept()

    def _loading_finished(self) -> None:
        """
        Used when the FileLoader returns the file
        Gets the PlaybackLoggerFile from the _data_channel, deletes the loader, and starts building things like the
        timeline, connecting the UI to work, and things like that
        :return:
        """
        self.play_back_loggerfile: PlaybackLoggerFile = self._data_channel.get()
        del self.loader
        self.make_timeline()
        self.length_logger_file = self.play_back_loggerfile.playback_manager.logger_pdus[-1][1]
        self._display_time(0)

        self.setWindowTitle(self._base_window_title + " - " + self.play_back_loggerfile.logger_path.split("/")[-1])

        self._connect_ui()

        self._set_playback_speed(1)
        self.play_back_loggerfile.set_exercise_id(self.exercise_id)

        # Enable GUI buttons
        self.buttonPlay.setDisabled(False)
        self.buttonStop.setDisabled(True)
        self.buttonPause.setDisabled(True)
        self.buttonJumpStart.setDisabled(True)
        self.buttonIncreaseSpeed.setDisabled(False)

        self.buttonDisconnect.setDisabled(False)

    def _timer_timeout(self) -> None:
        """
        The time displayed underneath, and by the timeline is merely an approximation of where the playback actually is.
        It's a pretty good one, but still an approximation. This is called once every 10 milliseconds, so the updates
        are almost imperceptible.
        This method also stops playback upon reaching the end of the loggerfile
        :return: None
        """
        # This is the time -0.5s * playback speed in order to catch it BEFORE the end, since reaching the end is
        # undefined behaviour
        if self._approximate_current_packettime >= self.play_back_loggerfile.playback_manager._maximum_time - (
                0.5 * self.playback_speed):
            self._stop()
        self._approximate_current_packettime += self._change_playback_position_timer.interval() * self.playback_speed / 1000

        timeline_position = self.time_mapper(self._approximate_current_packettime)
        self.TimeLine._calculate_mouse(timeline_position)
        self.TimeLine._trigger_refresh()

        self.change_position_by_time(self._approximate_current_packettime)

    def _connect_ui(self) -> None:
        """
        This method is called by the __init__(), and connects the buttons to the relevant methods
        :return: None
        """
        if self._ui_connected:
            return
        self.buttonPlay.clicked.connect(self._play)
        self.buttonStop.clicked.connect(self._stop)
        self.buttonPause.clicked.connect(self._pause)
        self.buttonJumpStart.clicked.connect(self._jump)

        self.buttonIncreaseSpeed.clicked.connect(self._increase_speed)
        self.buttonDecreaseSpeed.clicked.connect(self._decrease_speed)

        self.buttonConnect.clicked.connect(self._playback_connect)
        self.buttonDisconnect.clicked.connect(self._playback_disconnect)

        self._ui_connected = True

    def _setup_shortcuts(self) -> None:
        """
        Keyboard shortcuts are nice. This sets up non-alt based shortcuts, such as Ctrl+O to open a file
        :return:
        """
        self.shortcut_open = QShortcut(QKeySequence("Ctrl+O"), self)
        self.shortcut_open.activated.connect(self._choose_file)

    def _play(self) -> None:
        """
        Starts playback, either by un-pausing or straight-up starting from stopped, and sets which buttons can now be
        pressed
        :return: None
        """
        self.buttonPlay.setDisabled(True)
        self.buttonStop.setDisabled(False)
        self.buttonPause.setDisabled(False)
        self.buttonJumpStart.setDisabled(False)

        self.play_back_loggerfile.play()
        self._change_playback_position_timer.start(10)

    def _stop(self) -> None:
        """
        Stops playback, removes entities from the field, and sets which buttons can now be pressed.
        Additionally, moves the displayed approximate times to the actual times as returned by the PlaybackLoggerFile
        :return: None
        """
        stopped_playback = datetime.datetime.now().timestamp()
        self.buttonPlay.setDisabled(False)
        self.buttonStop.setDisabled(True)
        self.buttonPause.setDisabled(True)

        time.sleep(0.3)
        self.play_back_loggerfile.stop()
        self._set_timeline_position(stopped_playback)

        self._change_playback_position_timer.stop()

    def _pause(self) -> None:
        """
        Pauses playback, and sets which buttons can now be pressed.
        :return: None
        """
        stopped_playback = datetime.datetime.now().timestamp()
        self.buttonPlay.setDisabled(False)
        self.buttonStop.setDisabled(True)
        self.buttonPause.setDisabled(True)
        self.buttonJumpStart.setDisabled(True)

        self.play_back_loggerfile.pause()
        self._set_timeline_position(stopped_playback)

        self._change_playback_position_timer.stop()

    def _jump(self) -> None:
        """
        jumps to the start of the timeline and stops it from playing
        :return: None
        """
        self._stop()
        self._approximate_current_packettime = 0
        self._display_time(self._approximate_current_packettime)
        self.play_back_loggerfile.move(0)
        self.change_position_by_time(0)

    def _set_playback_speed(self, desired_speed: float) -> None:
        """
        Sets the playback speed to a specific float which is the speed in question
        :param desired_speed: float
        :return: None
        """
        self.playback_speed = desired_speed
        self.play_back_loggerfile.set_playback_speed(self.playback_speed)
        self.labelPlaybackSpeed.setText(str(self.playback_speed) + ".0x")
        self.update()

    def _increase_speed(self) -> None:
        """
        Increases the playback speed, within acceptable limits (as hardcoded and chosen by Gidon Rosalki)
        From experience the maximum feasible speed is 10x, but I've never seen anyone use more than 5.
        :return: None
        """
        if self.playback_speed == 5:
            self.buttonIncreaseSpeed.setDisabled(True)
            return
        elif self.playback_speed == 4:
            self.buttonIncreaseSpeed.setDisabled(True)
        self.buttonDecreaseSpeed.setDisabled(False)
        self._set_playback_speed(self.playback_speed + 1)

    def _decrease_speed(self) -> None:
        """
        Decreases the playback speed, down to 1x which is just normal speed playback.
        :return: None
        """
        if self.playback_speed == 1:
            self.buttonDecreaseSpeed.setDisabled(True)
            return
        elif self.playback_speed == 2:
            self.buttonDecreaseSpeed.setDisabled(True)
        self.buttonIncreaseSpeed.setDisabled(False)
        self._set_playback_speed(self.playback_speed - 1)

    def _playback_disconnect(self) -> None:
        """
        Stops playback, disables all button aside from connect, and enables the connect button and the ExerciseID
        chooser.
        :return: None
        """
        if self.buttonStop.isEnabled():
            self._stop()
        self.buttonPlay.setDisabled(True)
        self.buttonStop.setDisabled(True)
        self.buttonPause.setDisabled(True)

        self.buttonDisconnect.setDisabled(True)
        self.spinBoxExerciseId.setDisabled(False)
        self.buttonConnect.setDisabled(False)

    def _playback_connect(self) -> None:
        """
        Reconnects the playback to the network, re-enables the ability to run the playback, sets the ExerciseID to the
        chosen ExerciseID.
        :return: None
        """
        self.exercise_id = int(self.spinBoxExerciseId.text())
        self.play_back_loggerfile.set_exercise_id(self.exercise_id)

        self.buttonConnect.setDisabled(True)
        self.buttonDisconnect.setDisabled(False)
        self.spinBoxExerciseId.setDisabled(True)

        self.buttonPlay.setDisabled(False)

    def _set_timeline_position(self, stopped_playback_time: float) -> None:
        """
        Get the time that the PlaybackLoggerFile has returned that it says it has reached, and display it instead of the
        approximate times.
        Takes the unix timestamp at which you pressed the stop button, or rather, requested it to stop playing back.
        :param stopped_playback_time: float
        :return: None
        """
        while stopped_playback_time > self.play_back_loggerfile.playback_manager.stop_time:
            time.sleep(0.1)
        # TODO need a better way of getting the current time
        current_time = self.play_back_loggerfile.playback_manager.logger_pdus[
            self.play_back_loggerfile.playback_manager.position_pointer][1]
        print(current_time)
        self.change_position_by_time(current_time)

    def make_timeline(self) -> None:
        """
        Creates the TimeLine object.
        :return: None
        """
        self.TimeLine = _Timeline()
        self.TimeLine.selected_mouse_position.connect(self._updated_position)
        self.TimeLine.max_size.connect(self._changed_size)
        self.TimeLine.current_mouse_position.connect(self._moved_mouse)
        self.verticalLayout.addWidget(self.TimeLine)

    def _choose_file(self) -> None:
        """
        Handles picking a file, and setting up the handler for receiving the loaded PlaybackLoggerFile from the
        FileLoader.
        :return: None
        """
        dlg = QFileDialog()
        dlg.setFileMode(QFileDialog.AnyFile)

        self._delete_timeline()

        # region set_available_buttons
        self.buttonPlay.setDisabled(True)
        self.buttonStop.setDisabled(True)
        self.buttonPause.setDisabled(True)
        self.buttonJumpStart.setDisabled(True)

        self.buttonIncreaseSpeed.setDisabled(True)
        self.buttonDecreaseSpeed.setDisabled(True)

        self.buttonDisconnect.setDisabled(True)
        self.spinBoxExerciseId.setDisabled(True)
        self.buttonConnect.setDisabled(True)
        # endregion

        if dlg.exec_():
            filenames = dlg.selectedFiles()
            if len(filenames) < 1:
                print("No file selected")
                return None
            elif len(filenames) > 1:
                print("You selected more than one file, the first will be loaded")
            self.logger_file_name = filenames[0]
            self.preciseTime.setText(f"Loading: {filenames[0]}")

            self.loader = FileLoader(self.logger_file_name, self.exercise_id, self._data_channel)
            self.loader.finished.connect(self._loading_finished)
            self.loader.start()

    def _delete_timeline(self) -> None:
        """
        When opening a second logger file, the old one's stuff needs to be deleted. This method does that.
        :return: None
        """
        self.verticalLayout.removeWidget(self.TimeLine)
        # Shuts down the sender to make sure it doesn't panic because there's no one there
        if self.play_back_loggerfile is not None:
            self.play_back_loggerfile.playback_manager.message_queue.send(("exit",))
        del self.play_back_loggerfile, self.TimeLine

    def _display_time(self, position: float) -> None:
        """
        Displays the time in the text below the timeline
        :param position: float
        :return: None
        """
        self.preciseTime.setText(f"Current: {position:.2f}s | Length: {self.length_logger_file:.2f}s")

    def change_position_by_time(self, time: float) -> None:
        """
        Sets the position of hte Timeline, and the PlaybackLoggerFile according to a provided time in seconds from the
        start of the exercise
        :param time: float
        :return: None
        """
        timeline_position = self.time_mapper(time)
        self.TimeLine._calculate_mouse(timeline_position)
        # Not using updated position because this also moves the play_back_loggerfile
        # TODO combine updated_position and moved_mouse into one function with a boolean flag for moving the logger
        self._moved_mouse(timeline_position)
        self.TimeLine._trigger_refresh()

    def _updated_position(self, x_pos: int) -> None:
        """
        Takes an x position from the timeline, and from there figures out the time in seconds from the start of the log,
        and sets the position of the everything to be that.
        :param x_pos: int
        :return: None
        """
        if x_pos < 0:
            x_pos = 0
        elif x_pos > self.timeline_width:
            x_pos = self.timeline_width
        logger_position = self.position_mapper(x_pos)
        self._approximate_current_packettime = logger_position
        self._display_time(logger_position)
        self.play_back_loggerfile.playback_manager.remove_all_entities()
        self.play_back_loggerfile.move(logger_position)

    def _moved_mouse(self, x_pos: int) -> None:
        """
        When moving the mouse while the click button is pressed, you don't want to be updating the PlaybackLoggerFile
        every time you move a pixel, that's a waste of compute. Instead make it LOOK like you are by moving the timeline
        and the number underneath, but only actually move the PlaybackLoggerFile when you release.
        :param x_pos: int
        :return: None
        """
        if x_pos < 0:
            x_pos = 0
        elif x_pos > self.timeline_width:
            x_pos = self.timeline_width
        logger_position = self.position_mapper(x_pos)
        self._approximate_current_packettime = logger_position
        self._display_time(logger_position)

    def _changed_size(self, width: int) -> None:
        """
        When the window size is changed, so too is the width of the timeline, therefore the interpolations between width
        and seconds of loggerfile must be recalculated.
        :param width: int : pixels
        :return: None
        """
        self.timeline_width = width
        self.position_mapper = interp1d([0, width], [0, self.length_logger_file])
        self.time_mapper = interp1d([0, self.length_logger_file], [0, width])


def main():
    app = QApplication(sys.argv)
    form = DataExporter()
    form.show()
    app.exec_()


if __name__ == "__main__":
    main()
