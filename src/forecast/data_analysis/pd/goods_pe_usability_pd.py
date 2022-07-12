
def get_goods_pe_usable(pe, pc='pe', rc='r2', pt=-1, rt=0.5):
    return pe[(pe[pc] <= pt) & (pe[rc] >= rt)]
