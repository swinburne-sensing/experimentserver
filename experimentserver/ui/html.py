import flask

from .web import WebServer
from ..experiment.procedure_state import ProcedureTransition
from experimentserver.hardware import Hardware, HardwareTransition
from experimentserver.hardware.manager import HardwareManager
from experimentserver.util.module import get_all_subclasses


def _render_html(ui: WebServer, page: str, **kwargs):
    hardware_class_list = []

    # Generate list of hardware classes
    for hardware_class in get_all_subclasses(Hardware):
        class_fqn = hardware_class.__module__ + '.' + hardware_class.__qualname__

        # Strip module name
        class_fqn = class_fqn.split('.')
        class_fqn = class_fqn[3:]
        class_fqn = '.'.join(class_fqn)

        try:
            parameter_list = hardware_class.get_hardware_parameter_metadata()
            measurement_list = hardware_class.get_hardware_measurement_metadata()

            hardware_class_list.append({
                'class': class_fqn,
                'author': hardware_class.get_author(),
                'description': hardware_class.get_hardware_class_description(),
                'parameters': {k: v.description for k, v in parameter_list.items()},
                'measurements': {k: v.description for k, v in measurement_list.items()}
            })
        except NotImplementedError:
            continue

    hardware_list = []
    active_hardware = ui.get_procedure().get_active_hardware()

    # Generate list of hardware
    for identifier, manager in HardwareManager.get_all_instances().items():
        hardware = manager.get_hardware()

        class_fqn = hardware.__class__.__module__ + '.' + hardware.__class__.__qualname__

        # Strip module name
        class_fqn = class_fqn.split('.')
        class_fqn = class_fqn[3:]
        class_fqn = '.'.join(class_fqn)

        parameter_list = hardware.get_hardware_parameter_metadata()
        measurement_list = hardware.get_hardware_measurement_metadata()

        hardware_list.append({
            'active': identifier in active_hardware,
            'class': class_fqn,
            'author': hardware.get_author(),
            'identifier': identifier,
            'state': manager.get_state(),
            'description': hardware.get_hardware_instance_description(),
            'class_description': hardware.get_hardware_class_description(),
            'parameters': {k: v.description for k, v in parameter_list.items()},
            'measurements': {k: v.description for k, v in measurement_list.items()}
        })

    return flask.render_template(page, metadata=ui.get_metadata(), hardware=hardware_list,
                                 hardware_class=hardware_class_list, **kwargs)


def register_html(ui: WebServer):
    @ui.app.route('/')
    def page_index():
        return _render_html(ui, 'index.html')

    @ui.app.route('/debug')
    def page_debug():
        return _render_html(ui, 'debug.html', hardware_transitions=[x.value for x in HardwareTransition],
                            procedure_transitions=[x.value for x in ProcedureTransition])
