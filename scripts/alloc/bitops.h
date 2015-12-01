/*
 * Copyright (c) 2015-2016 Alexander Merritt.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of Alexander Merritt may not be used to endorse or promote
 *    products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY Alexander Merritt ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL Alexander
 * Merritt BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * Bit string operations intended for bit strings larger than a single
 * primitive type (e.g., many hundreds of bits).
 *
 * Bitsets ordered like so:
 *      +---+---+---+---+
 *      |n-1|  ...  | 0 |
 *      +---+---+---+---+
 * The 'bit' parameter is zero-based.
 */

#pragma once

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>

typedef uint64_t        bitchunk_t;
#define CHUNK_SHIFT     6       // to bits, not bytes
#define CHUNK_NBITS     (1UL<<CHUNK_SHIFT)
#define CHUNK_MASK      (CHUNK_NBITS-1)

typedef struct {
    bitchunk_t *map;
    off_t len, nbits;
} bitset_t;

static inline int
bitops_alloc(bitset_t *set, off_t nbits)
{
    if (!set) return 1;

    // round up a page
    set->len = (((nbits >> 3) >> 12) + 1) << 12;
    set->nbits = set->len << 3;

    set->map = (bitchunk_t*)mmap(NULL, set->len, PROT_READ|PROT_WRITE,
            MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    return (set->map == MAP_FAILED);
}

static inline void
bitops_free(bitset_t *set)
{
    if (!set)
        return;
    if (set->map)
        munmap(set->map, set->len);
    memset(set, 0, sizeof(*set));
}

/* val: 0 or 1 only */
static inline void
bitops_setall(bitset_t *set, int val)
{
    val = (val & 0x1) ? 0xff : 0x00;
    memset(set->map, val, set->len);
}

static inline void
bitops_set(bitset_t *set, off_t bit)
{
    off_t idx = bit >> CHUNK_SHIFT;
    off_t m   = bit % CHUNK_NBITS;
    off_t l   = (1 << CHUNK_SHIFT) - 1 - m;
    set->map[idx] |= (1UL << l);
}

static inline void
bitops_clear(bitset_t *set, off_t bit)
{
    off_t idx = bit >> CHUNK_SHIFT;
    off_t m   = bit % CHUNK_NBITS;
    off_t l   = (1 << CHUNK_SHIFT) - 1 - m;
    set->map[idx] &= ~(1UL << l) & CHUNK_MASK;
}

#if 0
static inline int
bitops_isset(bitset_t set, off_t nbits, off_t bit)
{
    const int n = (nbits >> 3); // number of chunks
	int nc = (bit >> 3);
	int c = (n - 1 - nc);
	int b = (bit - (nc << 3));
	return !!(set[c] & ((1<<b) & 0xff));
}

/* return index of the Nth set bit */
static inline int
bitops_nth_set(bitset_t set, off_t nbits, off_t bit)
{
    const int n = (nbits >> 3); // number of chunks
	int c, nth = 0, idx = 0;
	if (bit < 1) /* no such thing as zeroth bit */
		return (-1);
	for (c = (n-1); c >= 0; c--) {
		idx = (n-1-c) << 3;
		bitchunk_t ch = set[c];
		/* while still bits set to discover */
		while (ch) {
			/* skip zeros */
			while (ch && !(ch & 0x1)) {
				ch = ch >> 1;
				idx++;
			}
			assert(ch); // XXX
			/* found a bit */
			nth++;
			/* done? */
			if (nth == bit)
				return (idx);
			ch = ch >> 1;
			idx++;
		}
	}
	return (-1);
}
#endif

static inline off_t
bitops_ffset(bitset_t *set)
{
    const off_t nchunks = set->nbits >> CHUNK_SHIFT;
    off_t pos = 0L, c;

    for (c = 0L; c < nchunks; c++) {
        if (set->map[c] == 0UL)
            continue;
        // find count of trailing zeros (from least significant bit)
        __asm__ __volatile__ (
                "tzcnt %%rbx,%%rax;"
                : "+a"(pos)
                : "b"(set->map[c])
                : "%rax", "%rbx"
                );
        break;
    }

    if (c >= nchunks)
        return -1L;

    pos = CHUNK_NBITS - 1 - pos; // invert pos within chunk
    pos = (c << CHUNK_SHIFT) + pos; // calculate absolute offset
    return pos;
}
