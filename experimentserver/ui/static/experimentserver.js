function escapeHtml(unsafe) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

$(document).ready(function() {
    // Shutdown
    $("#shutdown-button").click(function () {
        $.ajax({
            url: "/cmd/shutdown",
            success: function (result) {
                if (result.success) {
                    $("#shutdown-result").show();
                    $("#shutdown-result h3").html("Request successful");
                    $("#shutdown-result p").html(result.message);
                    $("#shutdown-control").hide();
                } else {
                    $("#shutdown-result").show();
                    $("#shutdown-result h3").html("Request failed");
                    $("#shutdown-result p").html(result.message);
                }
            },
            error: function () {
                UIkit.notification({message: 'Failed to process shutdown request', status: 'danger'})
            },
            timeout: 5000
        });
    });

    // Hardware state
    $("#hardware-dialog-open").click(function () {
        $.ajax({
            url: "/json/hardware/state",
            success: function (result) {
                // Clear existing entries
                $("#hardware-dialog-container-state").empty()

                // Add new rows
                result.forEach(function (item) {
                    $("#hardware-dialog-container-state").append("<tr><td>" + item.identifier + "</td><td><span class=\"uk-label\">" + item.state + "</span></td><td>" + item.message + "</td></tr>")
                })
            },
            error: function () {
                UIkit.notification({message: 'Failed to retrieve hardware state', status: 'danger'})
            }
        });

        $.ajax({
            url: "/json/hardware/class",
            success: function (result) {
                // Clear existing entries
                $("#hardware-dialog-container-class").empty()

                // Add new rows
                result.forEach(function (item) {
                    let parameter_list = [];
                    let measurement_list = [];

                    for (let key in item.parameter) {
                        parameter_list.push("<li><b>" + key + ':</b> ' + item.parameter[key] + "</li>");
                    }

                    for (let key in item.measurement) {
                        measurement_list.push("<li><b>" + key + ':</b> ' + item.measurement[key] + "</li>");
                    }

                    parameter_list = "<ul class=\"uk-list\">" + parameter_list.join('') + "</ul>";
                    measurement_list = "<ul class=\"uk-list\">" + measurement_list.join('') + "</ul>";

                    $("#hardware-dialog-container-class").append("<tr><td>" + item.class + "<br /><span class=\"uk-text-small uk-text-muted\">" + escapeHtml(item.author) + "</span></td><td>" + item.description + "</td><td>" + parameter_list + "</td><td>" + measurement_list + "</td></tr>")
                })
            },
            error: function () {
                UIkit.notification({message: 'Failed to retrieve hardware classes', status: 'danger'})
            }
        });
    });

    // Event log
    $("#event-dialog-open").click(function () {
        $.ajax({
            url: "/json/event",
            success: function (result) {
                // Clear existing event entries
                $("#event-dialog-container").empty()

                // Add new rows
                result.forEach(function (item) {
                    $("#event-dialog-container").append('<tr><td><time datetime="' + item.time + '"></time></td><td>' + item.level + '</td><td>' + item.thread + '</td><td>' + item.message + '</td></tr>')
                })

                // Render time _tags
                timeago.render(document.querySelectorAll('#event-dialog-container time'), 'en_US', {minInterval: 5});
            },
            error: function () {
                UIkit.notification({message: 'Failed to retrieve event log', status: 'danger'})
            }
        });
    });

    $("#event-dialog-clear").click(function () {
        $.ajax({
            url: "/json/event/clear",
            success: function () {
                UIkit.notification({message: 'Event log cleared'})
            },
            error: function () {
                UIkit.notification({message: 'Failed to clear event log', status: 'danger'})
            },
            timeout: 5000
        })
    });
});
