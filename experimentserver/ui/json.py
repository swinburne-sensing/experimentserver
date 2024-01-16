import functools
import os.path
from datetime import datetime

import flask
import jinja2
import transitions
from experimentlib.util.constant import FORMAT_TIMESTAMP_CONSOLE, FORMAT_TIMESTAMP_FILENAME
from experimentlib.util.generate import hex_str
from experimentlib.util.time import now

import experimentserver
from experimentserver.hardware.control import HardwareTransition
from experimentserver.hardware.manager import HardwareManager
from experimentserver.protocol import Procedure, ProcedureTransition
from experimentserver.ui.web import UserInterfaceError, WebServer


class ServerJSONException(experimentserver.ApplicationException):
    pass


def _json_response_wrapper(ui: WebServer):
    def outer_wrapper(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                value = func(*args, **kwargs)

                if type(value) is str:
                    ui.logger().info(value)

                    return flask.jsonify({
                        'success': True,
                        'message': value
                    })
                elif type(value) in (list, dict, tuple):
                    # ui.logger().debug(f"JSON payload {value!r}")

                    return flask.jsonify({
                        'success': True,
                        'data': value
                    })
                else:
                    raise UserInterfaceError(f"Unexpected return type in JSON wrapper: {value!r}")
            except experimentserver.ApplicationException as exc:
                ui.logger().exception('Application error generated via web interface', notify=False, event=False,
                                      exc_info=exc)

                return flask.jsonify({
                    'success': False,
                    'message': exc.get_user_str('<hr>')
                })
            except transitions.MachineError as exc:
                ui.logger().warning(str(exc), exc_info=exc)

                return flask.jsonify({
                    'success': False,
                    'message': exc.args[0]
                })
            except Exception as exc:
                ui.logger().exception('Unhandled exception generated via web interface', exc_info=exc)

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

    @ui.app.route('/server/state/next', methods=['GET', 'POST'])
    @_json_response_wrapper(ui)
    def server_state_next():
        index = flask.request.args.get('index', type=int)

        ui.get_procedure().queue_transition(ProcedureTransition.GOTO, index)

        return f"Stage {index + 1} set as next stage"

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
            duration = ui.get_procedure().get_procedure_duration()

            return f"Procedure valid and ready to start, estimated runtime: {duration!s}"
        elif transition == ProcedureTransition.START:
            completion = now() + ui.get_procedure().get_procedure_duration()

            return f"Procedure running, estimated completion {completion.strftime(FORMAT_TIMESTAMP_CONSOLE)}"
        elif transition == ProcedureTransition.STOP:
            return 'Procedure stopped and returned to setup state.'
        else:
            return f"{identifier} transitioned to state {state.value}"

    @ui.app.route('/server/event')
    @_json_response_wrapper(ui)
    def server_event():
        records = []

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
    def server_event_clear():
        ui.get_event_buffer().flush()

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
        ui.logger().info('Shutdown requested via web interface', notify=True)
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
                        f"{now().strftime(FORMAT_TIMESTAMP_FILENAME)}.yaml",
            'content_type': 'application/x-yaml',
            'content': ui.get_procedure().procedure_export()
        }

    @ui.app.route('/procedure/import', methods=['POST'])
    @_json_response_wrapper(ui)
    def procedure_import():
        if len(flask.request.files) != 1 or 'file' not in flask.request.files:
            raise UserInterfaceError('Missing procedure file')

        filename = flask.request.files['file'].filename
        _, file_ext = os.path.splitext(filename)

        ui.logger().info(f"Loading filename: {filename}")

        with flask.request.files['file'].stream as procedure_file:
            procedure_file_content = procedure_file.read()

        procedure_file_content = procedure_file_content.decode()

        ui.logger().info(f"Uploaded content:\n{procedure_file_content}")

        if file_ext.lower() == '.j2':
            template_env = jinja2.Environment(
                loader=jinja2.loaders.DictLoader({}),
                autoescape=jinja2.select_autoescape()
            )

            procedure_file_template = template_env.from_string(
                procedure_file_content,
                {
                    'hex_str': hex_str
                }
            )

            procedure_file_content = procedure_file_template.render()

            ui.logger().info(f"Rendered content:\n{procedure_file_content}")

        procedure_store_filename = os.path.join(
            experimentserver.CONFIG_PATH, f"procedure_upload_{now().strftime(FORMAT_TIMESTAMP_FILENAME)}.yaml")

        with open(procedure_store_filename, 'w', encoding='utf-8') as procedure_store_file:
            procedure_store_file.write(procedure_file_content)

        ui.logger().info(f"Saved prrocedure to: {procedure_store_filename}")

        # Complete import
        procedure = Procedure.procedure_import(procedure_file_content)

        ui.logger().info(f"Read procedure:\n{procedure.procedure_export(False)}")

        ui.set_procedure(procedure)

        return 'Procedure uploaded!'
