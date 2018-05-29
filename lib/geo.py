

def bboxes_intersect(rect1, rect2):
    '''
    Check if two bounding boxes intersect
    rects are assumed to be [xmin, ymin, xmax, ymax]
    '''
    return not (rect2[0] > rect1[2]
                or rect2[2] < rect1[0]
                or rect2[3] < rect1[1]
                or rect2[1] > rect1[3])
