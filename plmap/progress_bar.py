from __future__ import division



PROGRESS_BAR_LENGTH = 40


def generate_loading_string(completed_tasks, total_tasks):
    """  <percentage completed>% [< -- based on percentage completion>] Completed/Total
    """
    try:
        fraction_completed = (completed_tasks / total_tasks)
    except:
        fraction_completed = 1  # To avoid division by Zero
    percentage_complete = fraction_completed * 100
    dashes = int(PROGRESS_BAR_LENGTH * fraction_completed)
    blanks = PROGRESS_BAR_LENGTH - dashes
    bar = "[" + "-" * dashes + ">" + " " * blanks + "]"
    fraction_display = "%s/%s" % (completed_tasks, total_tasks)
    loading_string = "%s%% %s %s" % (percentage_complete, bar, fraction_display)
    return loading_string
