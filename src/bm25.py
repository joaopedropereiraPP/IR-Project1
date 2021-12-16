class bm25:
    def __init__(self, k, b, positions, dl, avdl):
        self.k = k
        self.b = b
        self.positions = positions
        self.tf = len(positions)
        self.dl = dl
        self.avdl = avdl
    
    def bm25_without_positions(self):
        dividend = (self.k + 1) * self.tf
        B = (1 - self.b) + self.b * (self.dl / self.avdl)
        divider = (self.k * B) + self.tf
        return str(dividend / divider)

    def bm25_with_positions(self):
        dividend = (self.k + 1) * self.tf
        B = (1 - self.b) + self.b * (self.dl / self.avdl)
        divider = (self.k * B) + self.tf
        self.positions = ",".join(map(str, self.positions))

        return str(dividend / divider) + ":" + self.positions


