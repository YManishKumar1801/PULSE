LABELS = ("Positive", "Neutral", "Negative")


def confusion_matrix(y_true, y_pred, labels=LABELS):
    index = {label: position for position, label in enumerate(labels)}
    matrix = [[0 for _ in labels] for _ in labels]
    for actual, predicted in zip(y_true, y_pred):
        if actual in index and predicted in index:
            matrix[index[actual]][index[predicted]] += 1
    return matrix


def _safe_divide(numerator, denominator):
    return numerator / denominator if denominator else 0.0


def classification_report(y_true, y_pred, labels=LABELS):
    matrix = confusion_matrix(y_true, y_pred, labels)
    total = sum(sum(row) for row in matrix)
    correct = sum(matrix[i][i] for i in range(len(labels)))
    per_class = {}
    for position, label in enumerate(labels):
        true_positive = matrix[position][position]
        predicted_total = sum(matrix[row][position] for row in range(len(labels)))
        actual_total = sum(matrix[position])
        precision = _safe_divide(true_positive, predicted_total)
        recall = _safe_divide(true_positive, actual_total)
        f1 = _safe_divide(2 * precision * recall, precision + recall)
        per_class[label] = {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "support": actual_total,
        }
    present = [label for label in labels if per_class[label]["support"] > 0]
    macro = lambda key: round(sum(per_class[label][key] for label in present) / len(present), 4) if present else 0.0
    return {
        "n": total,
        "accuracy": round(_safe_divide(correct, total), 4),
        "macro_precision": macro("precision"),
        "macro_recall": macro("recall"),
        "macro_f1": macro("f1"),
        "per_class": per_class,
        "labels": list(labels),
        "confusion_matrix": matrix,
    }


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def group_reports(rows, group_key, min_support=20, labels=LABELS):
    groups = {}
    for row in rows:
        groups.setdefault(row[group_key], []).append(row)
    reports = {}
    for name, members in groups.items():
        report = classification_report([m["true"] for m in members], [m["pred"] for m in members], labels)
        report["reliable"] = len(members) >= min_support
        reports[name] = report
    return dict(sorted(reports.items(), key=lambda item: -item[1]["n"]))