import matplotlib as mpl
import numpy as np

from collections.abc import Iterable
from matplotlib import pyplot as plt
from matplotlib.patches import Patch


def catplot(data, title=None, xlabel=None, ylabel=None, colors=None, alpha=0.5, loc=None,
            width=0.7, ax=None, clip=True, xlim=None, ylim=None, markers=None,
            categories=None, samples=None, replicas=None, spacing=1.0):

    if ax is None:
        _, ax = plt.subplots()

    ticks = []
    tick_labels = []

    if data.index.levels == 3:
        categories = data.index.unique(0) if categories is None else categories
        samples = data.index.unique(1) if samples is None else samples
        replicas = data.index.unique(2) if replicas is None else replicas

    markers = ['o', 's', 'D', '^', '*', 'X', 'P', 'p', 'h', 'v'] if markers is None else markers
    position = 1
    palette = ['C' + str(i) for i in range(len(samples))] if colors is None else colors
    colors = dict(zip(samples, palette))

    swarms = []

    for category in categories:
        p_sum = 0
        p_count = 0
        c = 1
        for sample in samples:
            try:
                d = data.loc[(category, sample)]
            except KeyError:
                continue
            swarm = []
            ax.boxplot(d.values.flatten(), showfliers=False,
                       positions=[position], widths=width, boxprops=dict(color=colors[sample]),
                       medianprops=dict(color=colors[sample]))
            s = 0
            for replica in replicas:
                try:
                    d = data.loc[(category, sample, replica)]
                except KeyError:
                    continue
                y = d.values.flatten()
                if clip and ylim is not None:
                    y = np.clip(y, *ylim)
                x = [position for _ in range(len(y))]
                points = ax.scatter(x, y, color=colors[sample], alpha=alpha, marker=markers[s])
                swarm.append(points)
                s += 1
            p_sum += position
            p_count += 1
            c += 1
            position += 1
            swarms.append(swarm)
        ticks.append(p_sum / p_count)
        tick_labels.append(str(category))
        position += spacing

    ax.set_xticks(ticks)
    ax.set_xticklabels(tick_labels)
    if ylim is not None:
        ax.set_ylim(*ylim)
    if xlim is not None:
        ax.set_ylim(*xlim)

    for swarm in swarms:
        _swarmify(ax, swarm, width)

    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)

    handles = [Patch(facecolor='white', edgecolor=t[1], label=t[0]) for t in colors.items()]
    ax.legend(handles=handles, frameon=False, loc=loc)

    return ax


def _swarmify(ax, swarm, width, **kws):

    def could_overlap(xy_i, swarm, d):
        _, y_i = xy_i
        neighbors = []
        for xy_j in reversed(swarm):
            _, y_j = xy_j
            if (y_i - y_j) < d:
                neighbors.append(xy_j)
            else:
                break
        return np.array(list(reversed(neighbors)))

    def position_candidates(xy_i, neighbors, d):
        candidates = [xy_i]
        x_i, y_i = xy_i
        left_first = True
        for x_j, y_j in neighbors:
            dy = y_i - y_j
            dx = np.sqrt(max(d ** 2 - dy ** 2, 0)) * 1.05
            cl, cr = (x_j - dx, y_i), (x_j + dx, y_i)
            if left_first:
                new_candidates = [cl, cr]
            else:
                new_candidates = [cr, cl]
            candidates.extend(new_candidates)
            left_first = not left_first
        return np.array(candidates)

    def first_non_overlapping_candidate(candidates, neighbors, d):
        if len(neighbors) == 0:
            return candidates[0]

        neighbors_x = neighbors[:, 0]
        neighbors_y = neighbors[:, 1]
        d_square = d ** 2

        for xy_i in candidates:
            x_i, y_i = xy_i
            dx = neighbors_x - x_i
            dy = neighbors_y - y_i
            sq_distances = np.power(dx, 2.0) + np.power(dy, 2.0)
            good_candidate = np.all(sq_distances >= d_square)

            if good_candidate:
                return xy_i

        raise Exception('No non-overlapping candidates found. '
                        'This should not happen.')

    def beeswarm(orig_xy, d):
        midline = orig_xy[0, 0]
        swarm = [orig_xy[0]]
        for xy_i in orig_xy[1:]:
            neighbors = could_overlap(xy_i, swarm, d)
            candidates = position_candidates(xy_i, neighbors, d)
            offsets = np.abs(candidates[:, 0] - midline)
            candidates = candidates[np.argsort(offsets)]
            new_xy_i = first_non_overlapping_candidate(candidates, neighbors, d)
            swarm.append(new_xy_i)
        return np.array(swarm)

    def add_gutters(points, center, width):
        half_width = width / 2
        low_gutter = center - half_width
        off_low = points < low_gutter
        if off_low.any():
            points[off_low] = low_gutter
        high_gutter = center + half_width
        off_high = points > high_gutter
        if off_high.any():
            points[off_high] = high_gutter
        gutter_prop = (off_high + off_low).sum() / len(points)
        return points

    default_lw = mpl.rcParams["patch.linewidth"]
    default_s = mpl.rcParams["lines.markersize"] ** 2
    lw = kws.get("linewidth", kws.get("lw", default_lw))
    s = kws.get("size", kws.get("s", default_s))
    dpi = ax.figure.dpi
    d = (np.sqrt(s) + lw * 2) * (dpi / 72)

    if not isinstance(swarm, Iterable):
        swarm = [swarm]

    offsets = np.concatenate([points.get_offsets() for points in swarm])
    center = offsets[0, 0]
    sort = np.argsort(offsets[:, 1])

    orig_xy = ax.transData.transform(offsets[sort])
    new_xy = beeswarm(orig_xy, d)
    new_x, new_y = ax.transData.inverted().transform(new_xy).T

    add_gutters(new_x, center, width)
    new_offsets = np.c_[new_x, new_y]
    new_offsets = new_offsets[np.argsort(sort)]

    offset = 0
    for points in swarm:
        p_offsets = points.get_offsets()
        length = p_offsets.shape[0]
        p_new_offsets = new_offsets[offset:offset + length, :]
        points.set_offsets(p_new_offsets)
        offset = offset + length
