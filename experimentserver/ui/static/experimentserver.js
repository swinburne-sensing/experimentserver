// Escape HTML in text
function escapeHtml(unsafe) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function readableSeconds(seconds) {
    seconds = Math.round(seconds);

    if (seconds == 1) {
        return '1 second';
    } else {
        if (seconds <= 60) {
            return seconds.toString() + ' seconds';
        } else {
            let minutes = Math.floor(seconds / 60);
            seconds = seconds % 60;

            if (minutes <= 60) {
                return minutes.toString() + ':' + seconds.toString().padStart(2, "0");
            } else {
                let hours = Math.floor(minutes / 60)    ;
                minutes = minutes % 60;

                return hours.toString() + ':' + minutes.toString().padStart(2, "0") + ':' + seconds.toString().padStart(2, "0");
            }
        }
    }
}

// Notifications
function notifySuccess(title, message) {
    UIkit.notification({message: "<span class=\"uk-label uk-label-success\">" + title + "</span><hr>"+ message, pos: 'bottom-right'});
}

function notifyWarning(title, message) {
    UIkit.notification({message: "<span class=\"uk-label uk-label-warning\">" + title + "</span><hr>"+ message, pos: 'bottom-right', timeout: 30000});
}

function notifyError(title, message) {
    UIkit.notification({message: "<span class=\"uk-label uk-label-danger\">" + title + "</span><hr>"+ message, pos: 'bottom-right', timeout: 30000});
}

// Attach AJAX method to specified component
function registerAjax(parent, url, success_callback = null, data = null, error_message = null, disable_control = true) {
    function ajaxCall() {
        let parent = $(this);
        let payload = {};

        // Disable button
        if (disable_control)
            parent.prop('disabled', true);

        // Append any extra attributes to the payload
        if (parent.is('input') && parent.attr('type') === 'checkbox') {
            payload['enabled'] = parent.is(':checked');
        }

        if (data != null) {
            data.forEach(function (item) {
                payload[item] = parent.attr(item)
            });
        }

        $.ajax({
            url: url,
            data: payload,
            success: function (result) {
                // Enable button
                if (disable_control)
                    parent.prop('disabled', false);

                if (result.success) {
                    if ('message' in result) {
                        // Display message to user
                        notifySuccess('Command Accepted', result.message);
                    }

                    // Pass data to callback
                    if (success_callback != null)
                        success_callback(result);
                } else {
                    notifyWarning('Command Error', result.message);
                }
            },
            error: function () {
                // Enable button
                if (disable_control)
                    parent.prop('disabled', false);

                if (error_message == null) {
                    notifyError('Server Error', 'Error occurred while connecting to server.');
                } else {
                    notifyError('Server Error', error_message);
                }
            },
            timeout: 5000
        });
    }

    $(parent).click(ajaxCall);

    return ajaxCall
}

let updating = false;
let refresh_flag = false;

// Status and state updates
function updateStatus() {
    const dom_state = $("#procedure-state");
    const dom_status = $("#status");

    const dom_validate = $("#procedure-validate");
    const dom_start = $("#procedure-start");
    const dom_stop = $("#procedure-stop");

    const dom_previous = $("#procedure-previous");
    const dom_pause = $("#procedure-pause");
    const dom_repeat = $("#procedure-repeat");
    const dom_next = $("#procedure-next");
    const dom_finish = $("#procedure-finish");

    // Hardware state elements
    const dom_hardware_state = $(".es-hardware-state");

    // Editor controls
    const dom_procedure_edit = $(".procedure-edit");

    // Goto elements
    const dom_procedure_goto = $(".procedure-goto");

    // Stage elements
    const dom_stage = $(".es-stage");

    if (updating)
        console.log('Update already running');

    updating = true;

    $.ajax({
        url: '/server/state',
        success: function (result) {
            if (refresh_flag) {
                // Reload page
                location.reload();
            }

            // Clear state indicator
            dom_state
                .empty()
                .attr('class', 'uk-label');

            // Update status
            dom_status.html(result.data.status);

            // Update hardware state
            dom_hardware_state.each(function (index) {
                $(this).html(result.data.hardware_state[$(this).attr('identifier')])
            });

            // Disable controls
            $("#control button").prop('disabled', true);
            dom_procedure_edit.prop('disabled', true);
            dom_procedure_goto.prop('disabled', true);

            // Clear stage display
            dom_stage.removeClass('es-stage-current');
            dom_stage.removeClass('es-stage-next');

            const state = result.data.state;
            const stage_current = result.data.procedure.stage_current;
            const stage_next = result.data.procedure.stage_next;

            switch (state) {
                case 'setup':
                    dom_state
                        .html('Status: Setup')
                        .addClass('uk-label-danger');

                    dom_validate.prop('disabled', false);
                    dom_procedure_edit.prop('disabled', false);
                    break;

                case 'ready':
                    dom_state.html('Status: Ready');

                    dom_start.prop('disabled', false);
                    dom_stop.prop('disabled', false);
                    break;

                case 'running':
                    dom_state
                        .html('Status: Running')
                        .addClass('uk-label-success');

                    dom_stop.prop('disabled', false);
                    dom_previous.prop('disabled', false);
                    dom_pause.prop('disabled', false);
                    dom_repeat.prop('disabled', false);
                    dom_next.prop('disabled', false);
                    dom_finish.prop('disabled', false);

                    dom_procedure_goto.prop('disabled', false);
                    break;

                case 'paused':
                    dom_state
                        .html('Status: Paused')
                        .addClass('uk-label-warning');

                    dom_start.prop('disabled', false);
                    dom_stop.prop('disabled', false);
                    break;

                default:
                    notifyWarning('Unexpected response', 'Procedure state reported as ' + state);
                    break;
            }

            // Update stage
            const stages_data = result.data.stages;

            if (state == 'ready' || state === 'running' || state === 'paused') {
                dom_stage.each(function (index) {
                    let stage_index = $(this).attr('stage_index');
                    let stage_duration = $(this).find('.es-stage-duration-remaining');

                    if (stage_index == stage_current) {
                        $(this).addClass('es-stage-current');

                        // Update remaining time
                        let stage_data = stages_data[parseInt(stage_index)];
                        let stage_duration_remaining = stage_data.duration_remaining;

                        if (!isNaN(stage_data.duration_remaining))
                            stage_duration.html(readableSeconds(stage_data.duration_remaining));
                    } else if (stage_index == stage_next) {
                        $(this).addClass('es-stage-next');
                    }
                });
            }

            UIkit.modal('#comm-error-dialog').hide();
        },
        error: function () {
            // Show error dialog
            UIkit.modal('#comm-error-dialog').show();

            refresh_flag = true;
        },
        complete: function () {
            updating = false;
        },
        timeout: 10000
    })
}

function autoUpdateStatus() {
    updateStatus();
    setInterval(updateStatus, 3000);
}

$(document).ready(function() {
    // Shutdown
    registerAjax('#shutdown-button', '/server/shutdown', undefined, undefined, 'Failed to process shutdown request, server may already have shut down.', false);

    // Event log
    registerAjax('#event-dialog-open', '/server/event', function (result) {
        // Clear existing event entries
        $("#event-dialog-container").empty();

        // Add new rows
        result.data.forEach(function (item) {
            $("#event-dialog-container").append('<tr><td><time datetime="' + item.time + '"></time></td><td>' + item.level + '</td><td>' + item.thread + '</td><td>' + item.message + '</td></tr>')
        });

        // Render time _tags
        timeago.render(document.querySelectorAll('#event-dialog-container time'), 'en_US', {minInterval: 5});
    }, undefined, 'Failed to retrieve event log.', false);

    registerAjax('#event-dialog-clear', '/server/event/clear', undefined, undefined, 'Failed to clear event log.');

    // Procedure commands
    registerAjax("#control button", '/server/state/queue', function (result) {
        updateStatus();
    }, ['target', 'transition']);

    // Stage commands
    registerAjax(".es-stage-index button", '/server/state/next', function (result) {
        updateStatus();
    }, ['index']);

    // Enable/disable hardware
    registerAjax('.hardware-enable', '/procedure/hardware', undefined, ['identifier'], 'Unable to enable/disable hardware in procedure');

    // Trigger auto updates
    autoUpdateStatus();
});
