import functools
from datetime import datetime

import flask
import transitions

import experimentserver
from .web import UserInterfaceError, WebServer
from ..experiment.control import ProcedureTransition
from experimentserver.hardware import HardwareTransition
from experimentserver.hardware.manager import HardwareManager


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
                    ui.get_logger().debug(f"JSON payload {value!r}")

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
                ui.get_logger().warning(str(exc), notify=False, event=False, exc_info=exc)

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
            'state': ui.get_procedure().get_state().value,
            'hardware_active': ui.get_procedure().get_active_hardware(),
            'hardware_state': {identifier: manager.get_state().value for identifier, manager in
                               HardwareManager.get_all_instances().items()},
            'status': ui.get_procedure().get_status()
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

    @ui.app.route('/procedure/hardware', methods=['GET', 'POST'])
    @_json_response_wrapper(ui)
    def procedure_hardware():
        enabled = flask.request.args.get('enabled', type=str).lower() == 'true'
        identifier = flask.request.args.get('identifier')

        if identifier is None:
            raise UserInterfaceError('Missing hardware identifier')

        if enabled:
            ui.get_procedure().add_hardware(identifier)
            return f"Added {identifier} to procedure"
        else:
            ui.get_procedure().remove_hardware(identifier)
            return f"Removed {identifier} from procedure"

    @ui.app.route('/procedure/stage', methods=['GET', 'POST'])
    @_json_response_wrapper(ui)
    def procedure_stage():
        response = []
        index = 0

        for stage in ui.get_procedure().get_stages():
            stage_config = stage.get_config_html()

            response.append({
                'index': index,
                'type': stage.get_stage_type(),
                'config': stage_config,
                'duration': str(stage.get_duration())
            })

            index += 1

        return response
