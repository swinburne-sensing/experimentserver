// Escape HTML in text
function escapeHtml(unsafe) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
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
        let parent = $(this)
        let payload = {}

        // Disable button
        if (disable_control)
            parent.prop('disabled', true);

        // Append any extra attributes to the payload
        if (parent.is('input') && parent.attr('type') == 'checkbox') {
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

    const dom_hardware_state = $(".es-hardware-state");

    const dom_procedure_edit = $(".procedure-edit");

    if (updating)
        console.log('Update already running');

    updating = true;

    $.ajax({
        url: '/server/state',
        success: function (result) {
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

            let state = result.data.state;

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

            UIkit.modal('#comm-error-dialog').hide();
        },
        error: function () {
            // Show error dialog
            UIkit.modal('#comm-error-dialog').show();
        },
        complete: function () {
            updating = false;
        },
        timeout: 5000
    })
}

function autoUpdateStatus() {
    updateStatus();
    setTimeout(autoUpdateStatus, 1000);
}

$(document).ready(function() {
    // Shutdown
    registerAjax('#shutdown-button', '/server/shutdown', function (result) {
        $("#shutdown-result").show();
        $("#shutdown-result h3").html("Shutting down...");
        $("#shutdown-control").hide();
    }, undefined, 'Failed to process shutdown request, server may already have shut down.', false);

    // Event log
    registerAjax('#event-dialog-open', '/server/event', function (result) {
        // Clear existing event entries
        $("#event-dialog-container").empty()

        // Add new rows
        result.data.forEach(function (item) {
            $("#event-dialog-container").append('<tr><td><time datetime="' + item.time + '"></time></td><td>' + item.level + '</td><td>' + item.thread + '</td><td>' + item.message + '</td></tr>')
        })

        // Render time _tags
        timeago.render(document.querySelectorAll('#event-dialog-container time'), 'en_US', {minInterval: 5});
    }, undefined, 'Failed to retrieve event log.', false);

    registerAjax('#event-dialog-clear', '/server/event/clear', undefined, undefined, 'Failed to clear event log.');

    // Procedure commands
    registerAjax("#control button", '/server/state/queue', function (result) {
        updateStatus();
    }, ['target', 'transition']);

    // Enable/disable hardware
    registerAjax('.hardware-enable', '/procedure/hardware', undefined, ['identifier'], 'Unable to enable/disable hardware in procedure');

    // Trigger auto updates
    autoUpdateStatus();
});
