import functools
from datetime import datetime

import flask
import transitions

import experimentserver
from .web import UserInterfaceError, WebServer
from ..hardware import HardwareManager, HardwareTransition
from ..protocol import Procedure, ProcedureTransition, dump_yaml
from ..util.constant import FORMAT_TIMESTAMP_FILENAME


class ServerJSONException(experimentserver.ApplicationException):
    pass


def _json_response_wrapper(ui: WebServer):
    def outer_wrapper(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                value = func(*args, **kwargs)

                if type(value) is str:
                    ui.get_logger().info(value)

                    return flask.jsonify({
                        'success': True,
                        'message': value
                    })
                elif type(value) in (list, dict, tuple):
                    # ui.get_logger().debug(f"JSON payload {value!r}")

                    return flask.jsonify({
                        'success': True,
                        'data': value
                    })
                else:
                    raise UserInterfaceError(f"Unexpected return type in JSON wrapper: {value!r}")
            except experimentserver.ApplicationException as exc:
                ui.get_logger().exception('Application error generated via web interface', notify=False, event=False,
                                          exc_info=exc)

                return flask.jsonify({
                    'success': False,
                    'message': exc.get_user_str('<hr>')
                })
            except transitions.MachineError as exc:
                ui.get_logger().warning(str(exc), event=False, exc_info=exc)

                return flask.jsonify({
                    'success': False,
                    'message': exc.args[0]
                })
            except Exception as exc:
                ui.get_logger().exception('Unhandled exception generated via web interface', exc_info=exc)

                return flask.jsonify({
                    'success': False,
                    'message': str(exc)
                })

        return wrapper
    return outer_wrapper


def register_json(ui: WebServer):
    @ui.app.route('/server/state')
    @_json_response_wrapper(ui)
    def server_state():
        return {
            'hardware_state': {identifier: manager.get_state().value for identifier, manager in
                               HardwareManager.get_all_instances().items()},
            'procedure': ui.get_procedure().get_procedure_summary(),
            'stages':  ui.get_procedure().get_stages_summary(True),
            'state': ui.get_procedure().get_state().value
        }

    @ui.app.route('/server/state/queue', methods=['GET', 'POST'])
    @_json_response_wrapper(ui)
    def server_state_queue():
        target = flask.request.args.get('target', type=str)
        identifier = flask.request.args.get('identifier', type=str)
        transition = flask.request.args.get('transition', type=str)

        if transition is None:
            raise UserInterfaceError('Missing transition')

        try:
            if target.lower() == 'hardware':
                if identifier is None:
                    raise UserInterfaceError('Missing hardware identifier')

                manager = HardwareManager.get_instance(identifier)
                transition = HardwareTransition(transition)
            elif target.lower() == 'procedure':
                identifier = 'Procedure'
                manager = ui.get_procedure()
                transition = ProcedureTransition(transition)
            else:
                raise UserInterfaceError('Invalid or missing command target')
        except (KeyError, ValueError):
            raise UserInterfaceError(f"Invalid transition {transition}")

        # Apply transition
        state = manager.queue_transition(transition)

        if transition == ProcedureTransition.VALIDATE:
            return 'Procedure valid and ready to start.'
        elif transition == ProcedureTransition.START:
            return 'Procedure running.'
        elif transition == ProcedureTransition.STOP:
            return 'Procedure stopped and returned to setup state.'
        else:
            return f"{identifier} transitioned to state {state.value}"

    @ui.app.route('/server/event')
    @_json_response_wrapper(ui)
    def server_event():
        records = []

        for record in ui.get_event_buffer().get_events():
            records.insert(0, {
                'time': datetime.fromtimestamp(record.created).isoformat(),
                'level': record.levelname,
                'thread': record.threadName,
                'message': record.message
            })

        return records

    @ui.app.route('/server/event/clear')
    @_json_response_wrapper(ui)
    def server_event_clear():
        ui.get_event_buffer().clear()

        return 'Event log cleared'

    @ui.app.route('/server/shutdown')
    @_json_response_wrapper(ui)
    def server_shutdown():
        # Ask app thread to stop
        ui.get_thread().thread_stop()

        # Fetch server shutdown method
        shutdown_func = flask.request.environ.get('werkzeug.server.shutdown')

        if shutdown_func is None:
            raise UserInterfaceError('Not running with the Werkzeug Server, shutdown request ignored')

        # Call shutdown method
        ui.get_logger().info('Shutdown requested via web interface', event=True, notify=True)
        shutdown_func()

        return 'Shutting down, this page may now be closed.'

    @ui.app.route('/procedure')
    @_json_response_wrapper(ui)
    def procedure_dump():
        return ui.get_procedure().get_procedure_summary()

    @ui.app.route('/procedure/stages')
    @_json_response_wrapper(ui)
    def stages_dump():
        return ui.get_procedure().get_stages_summary()

    @ui.app.route('/procedure/export', methods=['GET', 'POST'])
    @_json_response_wrapper(ui)
    def procedure_export():
        return {
            'filename': f"procedure-{ui.get_procedure().get_uid()}-"
                        f"{datetime.now().strftime(FORMAT_TIMESTAMP_FILENAME)}.yaml",
            'content_type': 'application/x-yaml',
            'content': dump_yaml(ui.get_procedure().procedure_export())
        }

    @ui.app.route('/procedure/import', methods=['POST'])
    @_json_response_wrapper(ui)
    def procedure_import():
        if len(flask.request.files) != 1 or 'file' not in flask.request.files:
            raise UserInterfaceError('Missing procedure file')

        with flask.request.files['file'].stream as procedure_file:
            procedure_file_content = procedure_file.read()

        ui.get_logger().info(f"Uploaded content:\n{procedure_file_content}")

        procedure = Procedure.procedure_import(procedure_file_content.decode())

        ui.get_logger().info(f"Read procedure:\n{procedure.procedure_export(True)}")

        ui.set_procedure(procedure)

        return 'Procedure uploaded!'
