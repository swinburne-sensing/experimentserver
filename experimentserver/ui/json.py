import functools
import os.path
import typing
from datetime import datetime

import flask
import transitions
import yaml
from experimentlib.util.constant import FORMAT_TIMESTAMP_CONSOLE, FORMAT_TIMESTAMP_FILENAME
from experimentlib.util.generate import hex_str
from experimentlib.util.time import now

import experimentserver
from experimentserver.hardware.control import HardwareTransition
from experimentserver.hardware.manager import HardwareManager
from experimentserver.protocol import Procedure, ProcedureTransition
from experimentserver.protocol.file import HEADER
from experimentserver.ui.web import APIError, UserInterfaceError, WebServer


class ServerJSONException(experimentserver.ApplicationException):
    pass


TParam = typing.ParamSpec('TParam')
TReturn = typing.TypeVar('TReturn')

TJSONWrappable = typing.Union[
    str,
    typing.Dict[str, typing.Any],
    typing.List[typing.Any],
    typing.Tuple[typing.Any, ...],
]
TJSONWrapped = typing.Mapping[str, typing.Any]


def _json_response_wrapper(ui: WebServer):
    def outer_wrapper(func: typing.Callable[TParam, TJSONWrappable]) -> typing.Callable[TParam, TJSONWrapped]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                value = func(*args, **kwargs)

                if isinstance(value, str):
                    ui.logger().info(value)

                    return flask.jsonify({
                        'success': True,
                        'message': value
                    })
                elif type(value) in (list, dict, tuple):
                    return flask.jsonify({
                        'success': True,
                        'data': value
                    })
                else:
                    raise UserInterfaceError(f"Unexpected return type in JSON wrapper: {value!r}")
            except APIError as exc:
                ui.logger().exception('API error generated via web interface', notify=False, event=False,
                                      exc_info=exc)
                
                return flask.jsonify({
                    'success': False,
                    'message': '<b>API Error</b><hr>' + exc.get_user_str('<hr>')
                })
            except experimentserver.ApplicationException as exc:
                ui.logger().exception('Application error generated via web interface', notify=False, event=False,
                                      exc_info=exc)

                return flask.jsonify({
                    'success': False,
                    'message': '<b>Application Error</b><hr>' + exc.get_user_str('<hr>')
                })
            except transitions.MachineError as exc:
                ui.logger().warning(str(exc), exc_info=exc)

                return flask.jsonify({
                    'success': False,
                    'message': '<b>State Machine Error</b><hr>' + exc.args[0]
                })
            except Exception as exc:
                ui.logger().exception('Unhandled exception generated via web interface', exc_info=exc)

                return flask.jsonify({
                    'success': False,
                    'message': str(exc)
                })

        return wrapper
    return outer_wrapper


def register_json(ui: WebServer) -> None:
    @ui.app.route('/server/state')
    @_json_response_wrapper(ui)
    def server_state() -> TJSONWrappable:
        return {
            'hardware_state': {identifier: manager.get_state().value for identifier, manager in
                               HardwareManager.get_all_instances().items()},
            'procedure': ui.get_procedure().get_procedure_summary(),
            'stages':  ui.get_procedure().get_stages_summary(True),
            'state': ui.get_procedure().get_state().value
        }

    @ui.app.route('/server/state/next', methods=['GET', 'POST'])
    @_json_response_wrapper(ui)
    def server_state_next() -> TJSONWrappable:
        index = flask.request.args.get('index', type=int)

        if index is None:
            raise APIError('Missing index argument')

        ui.get_procedure().queue_transition(ProcedureTransition.GOTO, index)

        return f"Stage {index + 1} set as next stage"

    @ui.app.route('/server/state/queue', methods=['GET', 'POST'])
    @_json_response_wrapper(ui)
    def server_state_queue() -> TJSONWrappable:
        target = flask.request.args.get('target', type=str)
        identifier = flask.request.args.get('identifier', type=str)
        transition = flask.request.args.get('transition', type=str)

        if target is None:
            raise APIError('Missing target argument')

        if transition is None:
            raise APIError('Missing transition argument')

        final_state: str = 'unknown'

        try:
            if target.lower() == 'hardware':
                if identifier is None:
                    raise APIError('Missing hardware identifier')

                hardware_manager = HardwareManager.get_instance(identifier)
                hardware_manager.queue_transition(HardwareTransition(transition))
                final_state = hardware_manager.get_state().value
            elif target.lower() == 'procedure':
                identifier = 'Procedure'
                procedure_manager = ui.get_procedure()
                procedure_manager.queue_transition(ProcedureTransition(transition))
                final_state = procedure_manager.get_state().value
            else:
                raise UserInterfaceError('Invalid or missing command target')
        except (KeyError, ValueError):
            raise UserInterfaceError(f"Invalid transition {transition}")

        if transition == ProcedureTransition.VALIDATE:
            duration = ui.get_procedure().get_procedure_duration()

            return f"Procedure valid and ready to start, estimated runtime: {duration!s}"
        elif transition == ProcedureTransition.START:
            completion = now() + ui.get_procedure().get_procedure_duration()

            return f"Procedure running, estimated completion {completion.strftime(FORMAT_TIMESTAMP_CONSOLE)}"
        elif transition == ProcedureTransition.STOP:
            return 'Procedure stopped and returned to setup state.'
        else:
            return f"{identifier} transitioned to state {final_state}"

    @ui.app.route('/server/event')
    @_json_response_wrapper(ui)
    def server_event() -> TJSONWrappable:
        records: typing.List[typing.Dict[str, str]] = []

        for record in ui.get_event_buffer().records:
            records.insert(0, {
                'time': datetime.fromtimestamp(record.created).isoformat(),
                'level': record.levelname,
                'thread': record.threadName,
                'message': record.message
            })

        return records

    @ui.app.route('/server/event/clear')
    @_json_response_wrapper(ui)
    def server_event_clear() -> TJSONWrappable:
        ui.get_event_buffer().flush()

        return 'Event log cleared'

    @ui.app.route('/server/shutdown')
    @_json_response_wrapper(ui)
    def server_shutdown() -> TJSONWrappable:
        # Ask app thread to stop
        ui.get_thread().thread_stop()

        # Fetch server shutdown method
        shutdown_func = flask.request.environ.get('werkzeug.server.shutdown')

        if shutdown_func is None:
            raise UserInterfaceError('Not running with the Werkzeug Server, shutdown request ignored')

        # Call shutdown method
        ui.logger().info('Shutdown requested via web interface', notify=True)
        shutdown_func()

        return 'Shutting down, this page may now be closed.'

    @ui.app.route('/procedure')
    @_json_response_wrapper(ui)
    def procedure_dump() -> TJSONWrappable:
        return ui.get_procedure().get_procedure_summary()

    @ui.app.route('/procedure/stages')
    @_json_response_wrapper(ui)
    def stages_dump() -> TJSONWrappable:
        return ui.get_procedure().get_stages_summary()

    @ui.app.route('/procedure/export', methods=['GET', 'POST'])
    @_json_response_wrapper(ui)
    def procedure_export() -> TJSONWrappable:
        procedure_str: str = HEADER + yaml.dump(ui.get_procedure().procedure_export())

        return {
            'filename': f"procedure-{ui.get_procedure().get_uid()}-"
                        f"{now().strftime(FORMAT_TIMESTAMP_FILENAME)}.yaml",
            'content_type': 'application/x-yaml',
            'content': procedure_str
        }

    @ui.app.route('/procedure/import', methods=['POST'])
    @_json_response_wrapper(ui)
    def procedure_import() -> TJSONWrappable:
        if len(flask.request.files) != 1 or 'file' not in flask.request.files:
            raise UserInterfaceError('Missing procedure file')

        filename = flask.request.files['file'].filename

        if filename is None:
            raise APIError('Missing filename')

        _, file_ext = os.path.splitext(filename)

        ui.logger().info(f"Loading filename: {filename}")

        with flask.request.files['file'].stream as procedure_file:
            procedure_file_content_bytes = procedure_file.read()

        procedure_file_content = procedure_file_content_bytes.decode()

        ui.logger().info(f"Uploaded content:\n{procedure_file_content}")

        if file_ext.lower() == '.j2':
            procedure_file_content = Procedure.render_template(procedure_file_content)
            ui.logger().info(f"Rendered content:\n{procedure_file_content}")

        procedure_store_filename = os.path.join(
            experimentserver.CONFIG_PATH, 
            f"procedure_upload_{now().strftime(FORMAT_TIMESTAMP_FILENAME)}.yaml"
        )

        with open(procedure_store_filename, 'w', encoding='utf-8') as procedure_store_file:
            procedure_store_file.write(procedure_file_content)

        ui.logger().info(f"Saved prrocedure to: {procedure_store_filename}")

        # Complete import
        procedure = Procedure.procedure_import(procedure_file_content)

        ui.logger().info(f"Read procedure:\n{procedure.procedure_export(False)}")

        ui.set_procedure(procedure)

        return 'Procedure uploaded!'
