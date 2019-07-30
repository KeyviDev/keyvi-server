// -*- Mode: C++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
// Copyright 2008, The TPIE development team
// 
// This file is part of TPIE.
// 
// TPIE is free software: you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License as published by the
// Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.
// 
// TPIE is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
// License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with TPIE.  If not, see <http://www.gnu.org/licenses/>

///////////////////////////////////////////////////////////////////////////
/// \file kdtree.h
/// Providese definition and implementation of a blocked kd-tree.
///////////////////////////////////////////////////////////////////////////

#ifndef _TPIE_AMI_KDTREE_H
#define _TPIE_AMI_KDTREE_H

#include <tpie/config.h>

// Get definitions for working with Unix and Windows
#include <tpie/portability.h>
// For pair.
#include <utility>
// For stack.
#include <stack>
// For min, max.
#include <algorithm>
// For vector.
#include <vector>
// For priority_queue.
#include <queue>
// STL string.
#include <string>

#include <tpie/util.h>
// TPIE stuff.
#include <tpie/stream.h>
#include <tpie/scan.h>
#include <tpie/sort.h>
#include <tpie/coll.h>
#include <tpie/block.h>
// The stats_tree class.
#include <tpie/stats_tree.h>
// The cache manager.
#include <tpie/cache.h>
// The point/record classes.
#include <tpie/point.h>
// Supporting types: kdtree_status, kdtree_params, etc.
#include <tpie/kd_base.h>

namespace tpie {

namespace ami {

// Forward references.
template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL> class kdtree_leaf;
template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL> class kdtree_node;

/** A global object storing the default parameter values. */
const kdtree_params _kdtree_params_default = kdtree_params();

#define TPIE_AMI_KDTREE_HEADER_MAGIC_NUMBER 0xA9420E

#define TPLOG(msg) 
//  (LOG_APP_DEBUG(msg),LOG_FLUSH_LOG)


///////////////////////////////////////////////////////////////////////////
// The kdtree class.
///////////////////////////////////////////////////////////////////////////
template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node=kdtree_bin_node_default<coord_t, dim>, class BTECOLL = bte::COLLECTION >
class kdtree {
public:

	typedef record<coord_t, size_t, dim> point_t;
	typedef record<coord_t, size_t, dim> record_t;
	typedef point<coord_t, dim> key_t;
	typedef stream<point_t> stream_t;
	typedef collection_single<BTECOLL> collection_t;
	typedef kdtree_node<coord_t, dim, Bin_node, BTECOLL> node_t;
	typedef kdtree_leaf<coord_t, dim, BTECOLL> leaf_t;

	///////////////////////////////////////////////////////////////////////////
	/// Constructor.
	///////////////////////////////////////////////////////////////////////////
	kdtree(const kdtree_params& params = _kdtree_params_default);

	///////////////////////////////////////////////////////////////////////////
	/// Constructor; opens/creates a new kdtree with the given name, type
	/// and parameters.
	///////////////////////////////////////////////////////////////////////////
	kdtree(const std::string& base_file_name,
		   collection_type type = WRITE_COLLECTION, 
		   const kdtree_params& params = _kdtree_params_default);

	///////////////////////////////////////////////////////////////////////////
	/// Sorts \p in_stream on each of the dim coordinates and stores the
	/// sorted streams in the given array. If \p out_streams[i] is \p NULL, a
	/// new temporary stream is created.
	///////////////////////////////////////////////////////////////////////////
	err sort(stream_t* in_stream, stream_t* out_streams[]);

	///////////////////////////////////////////////////////////////////////////
	/// Bulk loads a kd-tree from the stream of points set during
	/// construction.
	///////////////////////////////////////////////////////////////////////////
	err load_sorted(stream_t* streams_s[],
					float lfill = 0.75, float nfill = 0.75,
					int load_method = TPIE_AMI_KDTREE_LOAD_SORT | TPIE_AMI_KDTREE_LOAD_GRID);

	///////////////////////////////////////////////////////////////////////////
	/// A shortcut for calling sort() followed by load_sorted().
	///////////////////////////////////////////////////////////////////////////
	err load(stream_t* in_stream, 
			 float lfill = 0.75, float nfill = 0.75,
			 int load_method = TPIE_AMI_KDTREE_LOAD_SORT | TPIE_AMI_KDTREE_LOAD_GRID);

	///////////////////////////////////////////////////////////////////////////
	/// Bulk load using sampling, thus avoiding the sorting step.
	///////////////////////////////////////////////////////////////////////////
	err load_sample(stream_t* in_stream);

	///////////////////////////////////////////////////////////////////////////
	/// Writes all points stored in the tree to the given stream. No
	/// changes are made to the tree.
	///////////////////////////////////////////////////////////////////////////
	err unload(stream_t* s);

	///////////////////////////////////////////////////////////////////////////
	// Reports the \p k nearest neighbors of point \p p.
	///////////////////////////////////////////////////////////////////////////
	TPIE_OS_OFFSET k_nn_query(const point_t &p, stream_t* stream, TPIE_OS_OFFSET k);

	///////////////////////////////////////////////////////////////////////////
	/// Reports all points inside the window determined by \p p1 and \p p2. If
	/// \p stream is \p NULL, only *count* the points inside the window. NB:
	/// Note that counting is much faster than reporting, because (a) no points are
	/// written out, and (b) the weights of nodes and leaves are used to
	/// speed up the search.
	///////////////////////////////////////////////////////////////////////////
	TPIE_OS_OFFSET window_query(const point_t& p1, const point_t& p2, stream_t* stream);

	///////////////////////////////////////////////////////////////////////////
	/// Finds a point within the tree; returns true if found, false otherwise.
	///////////////////////////////////////////////////////////////////////////
	bool find(const point_t& p);

	///////////////////////////////////////////////////////////////////////////
	/// (Tries to) insert a point. Return true if successful.
	///////////////////////////////////////////////////////////////////////////
	bool insert(const point_t& p);

	///////////////////////////////////////////////////////////////////////////
	/// Deletes a point. Returns true if found and deleted. No tree
	/// reorganizaton is performed. Leaves with no points are kept in the
	/// tree (to insure correctness of leaves' threading).
	///////////////////////////////////////////////////////////////////////////
	bool erase(const point_t& p);

	///////////////////////////////////////////////////////////////////////////
	/// Return the number of points stored in the tree.
	///////////////////////////////////////////////////////////////////////////
	TPIE_OS_OFFSET size() const { return header_.size; }

	///////////////////////////////////////////////////////////////////////////
	/// Sets the persistence. It passes per along to the two block
	/// collections.
	///////////////////////////////////////////////////////////////////////////
	void persist(persistence per);

	///////////////////////////////////////////////////////////////////////////
	/// Inquires the (real) parameters.
	///////////////////////////////////////////////////////////////////////////
	const kdtree_params& params() const { return params_; }

	///////////////////////////////////////////////////////////////////////////
	/// Inquire the status.
	///////////////////////////////////////////////////////////////////////////
	kdtree_status status() const { return status_; };

	///////////////////////////////////////////////////////////////////////////
	/// Inquires the \ref mbr_lo point.
	///////////////////////////////////////////////////////////////////////////
	point_t mbr_lo() const { return header_.mbr_lo; }

	///////////////////////////////////////////////////////////////////////////
	// Inquires the \ref mbr_hi point.
	///////////////////////////////////////////////////////////////////////////
	point_t mbr_hi() const { return header_.mbr_hi; }

	///////////////////////////////////////////////////////////////////////////
	// Inquires the statistics.
	///////////////////////////////////////////////////////////////////////////
	const stats_tree &stats();

	///////////////////////////////////////////////////////////////////////////
	/// Prints out some stuff about the tree structure. For debugging
	/// purposes only.
	///////////////////////////////////////////////////////////////////////////
	void print(std::ostream& s);

	///////////////////////////////////////////////////////////////////////////
	/// Prints the BINARY kd-tree indented.
	///////////////////////////////////////////////////////////////////////////
	void print(std::ostream& s, bool print_mbr, bool print_level, char indent_char = ' ');

	///////////////////////////////////////////////////////////////////////////
	/// Inquires the base path name.
	///////////////////////////////////////////////////////////////////////////
	const std::string& name() const { return name_; }

	///////////////////////////////////////////////////////////////////////////
	/// Destructor.
	///////////////////////////////////////////////////////////////////////////
	~kdtree();

	///////////////////////////////////////////////////////////////////////////
	// Inquires the number of Bin_node's.
	///////////////////////////////////////////////////////////////////////////
	TPIE_OS_OFFSET bin_node_count() const { return bin_node_count_; }

	///////////////////////////////////////////////////////////////////////////
	/// Metainformation about the tree.
	///////////////////////////////////////////////////////////////////////////
	class header_t {
	public:
		unsigned int magic_number;
		point_t mbr_lo;
		point_t mbr_hi;
		bid_t root_bid;
		TPIE_OS_OFFSET size;
		link_type_t root_type;
		unsigned char store_weights;
		unsigned char use_exact_split;
		unsigned char use_kdbtree_leaf;
		unsigned char use_real_median;

		header_t():
			magic_number(TPIE_AMI_KDTREE_HEADER_MAGIC_NUMBER), mbr_lo(0), mbr_hi(0), 
			root_bid(0), size(0), root_type(BLOCK_LEAF), 
			store_weights(TPIE_AMI_KDTREE_STORE_WEIGHTS), 
			use_exact_split(TPIE_AMI_KDTREE_USE_EXACT_SPLIT), 
			use_kdbtree_leaf(TPIE_AMI_KDTREE_USE_KDBTREE_LEAF),
			use_real_median(TPIE_AMI_KDTREE_USE_REAL_MEDIAN) {}
	};


protected:

	///////////////////////////////////////////////////////////////////////////
	/// Function object for the node cache write out.
	///////////////////////////////////////////////////////////////////////////
	class remove_node {
	public:
		void operator()(node_t* p) { delete p; }
	};

	///////////////////////////////////////////////////////////////////////////
	/// Function object for the leaf cache write out.
	///////////////////////////////////////////////////////////////////////////
	class remove_leaf { 
	public:
		void operator()(leaf_t* p) { delete p; }
	};

	typedef tpie::ami::CACHE_MANAGER<node_t*, remove_node> node_cache_t;
	typedef tpie::ami::CACHE_MANAGER<leaf_t*, remove_leaf> leaf_cache_t;

	/** The node cache. */
	node_cache_t* node_cache_;
	/** The leaf cache. */
	leaf_cache_t* leaf_cache_;

	/** The collection storing the leaves. */
	collection_t* pcoll_leaves_;

	/** The collection storing the internal nodes (could be the same). */
	collection_t* pcoll_nodes_;

	/** Critical information: root bid_t and type, mbr, size (will be
	 * stored into the header of the nodes collection). */
	header_t header_;

	/** This points to the first leaf in the order given by the next
	 * pointers stored in leaves. Used by unload to start the leaf
	 * traversal. */
	bid_t first_leaf_id_;

	leaf_t* previous_leaf_;

	/** The status. */
	kdtree_status status_;

	/** Run-time parameters. */
	kdtree_params params_;

	/** One comparison object for each dimension. */
	typename point_t::cmp* comp_obj_[dim];

	/** Statistics object. */
	stats_tree stats_;

	/** The total number of bin nodes. */
	TPIE_OS_OFFSET bin_node_count_;

	/** Base path name. */
	std::string name_;

	/** Various initialization common to all constructors. */
	void shared_init(const std::string& base_file_name, collection_type type);

	TPIE_OS_OFFSET real_median(TPIE_OS_OFFSET sz) { return (sz - 1) / 2; }
	TPIE_OS_SIZE_T real_median(TPIE_OS_SIZE_T sz) { return (sz - 1) / 2; }

	///////////////////////////////////////////////////////////////////////////
	/// Returns the position of the median point.
	///////////////////////////////////////////////////////////////////////////
	TPIE_OS_OFFSET median(TPIE_OS_OFFSET sz) {
#if TPIE_AMI_KDTREE_USE_REAL_MEDIAN
		return real_median(sz);
#else
		int i = 0;
		while ((static_cast<TPIE_OS_OFFSET>(1) << i) < 
			   static_cast<TPIE_OS_OFFSET>((sz + params_.leaf_size_max - 1) / params_.leaf_size_max))
			i++;
		return  (static_cast<TPIE_OS_OFFSET>(1) << (i-1)) * params_.leaf_size_max - 1;
#endif
	}

	///////////////////////////////////////////////////////////////////////////
	// Returns the position of the median point.
	///////////////////////////////////////////////////////////////////////////
	TPIE_OS_SIZE_T median(TPIE_OS_SIZE_T sz) {
#if TPIE_AMI_KDTREE_USE_REAL_MEDIAN
		return real_median(sz);
#else
		int i = 0;
		while ((static_cast<TPIE_OS_SIZE_T>(1) << i) < ((sz + params_.leaf_size_max - 1) / 
						   params_.leaf_size_max)) 
			i++;
		return  (static_cast<TPIE_OS_SIZE_T>(1) << (i-1)) * params_.leaf_size_max - 1;
#endif
	}

	TPIE_OS_SIZE_T max_intranode_height(bid_t bid) {
		return (bid == header_.root_bid ? 
				params_.max_intraroot_height: 
				params_.max_intranode_height);
	}

	///////////////////////////////////////////////////////////////////////////
	/// Used during binary bulk loading to pass parameters in the recursion.
	///////////////////////////////////////////////////////////////////////////
	class bn_context {
	public:
		bn_context() {}
		bn_context(TPIE_OS_SIZE_T _i, TPIE_OS_SIZE_T _h, TPIE_OS_SIZE_T _d): 
			i(_i), h(_h), d(_d) {}
		/** the index of the current bin node. */
		TPIE_OS_SIZE_T i; 
		/** the depth (height) of the current bin node. */
		TPIE_OS_SIZE_T h; 
		/** the split dimension of the current bin node. */ 
		TPIE_OS_SIZE_T d; 
	};

	// Forward reference.
	class grid;

	///////////////////////////////////////////////////////////////////////////
	/// The grid matrix containing the cell counts of a sub-grid.
	///////////////////////////////////////////////////////////////////////////
	class grid_matrix {
	public:
		/** The grid to which this matrix refers to. */ 
		grid* g;
		/** The number of strips in g spanned by this sub-grid. */
		TPIE_OS_SIZE_T gt[dim];
		/** The coordinates of the grid lines relative to g. The real
		 * coordinates: g->l[i][gl[i]] */
		TPIE_OS_SIZE_T gl[dim];
		/** The grid counts. It's an array of length sz (the number of cells). */
		TPIE_OS_SIZE_T* c;
		/** Total number of cells: gt[0] * gt[1] *...* gt[dim-1]. */
		TPIE_OS_SIZE_T sz;
		/** Total number of points represented by this sub-grid. */
		TPIE_OS_OFFSET point_count;
		/** The low and high coordinates. The boolean bit is false iff the
		 * value is unbounded on that dimension. */
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
		std::pair<point_t, bool> lo[dim];
		std::pair<point_t, bool> hi[dim];
#else
		std::pair<coord_t, bool> lo[dim];
		std::pair<coord_t, bool> hi[dim];
#endif

		///////////////////////////////////////////////////////////////////////////
		/// Constructs a grid_matrix.
		///////////////////////////////////////////////////////////////////////////
		grid_matrix(TPIE_OS_SIZE_T* tt, grid *gg) {
			size_t i;
			sz = 1;
			for (i = 0; i < dim; i++) {
				gt[i] = tt[i];
				gl[i] = 0;
				sz *= gt[i];
				lo[i].second = false;
				hi[i].second = false;
			}
			g = gg;
			point_count = g->point_count;
			c = NULL;
		}
    
		///////////////////////////////////////////////////////////////////////////
		/// Copy constructor.
		///////////////////////////////////////////////////////////////////////////
		grid_matrix(const grid_matrix& x) {
			size_t i;
			sz = 1;
			for (i = 0; i < dim; i++) {
				gt[i] = x.gt[i];
				gl[i] = x.gl[i];
				lo[i] = x.lo[i];
				hi[i] = x.hi[i];
			}
			sz = x.sz;
			g = x.g;
			c = NULL;
		}

		///////////////////////////////////////////////////////////////////////////
		/// Splits along strip \p s orthogonal to dimension \p d. The lower
		/// coordinates are kept here, and the high ones are returned in a
		/// newly created object.
		///////////////////////////////////////////////////////////////////////////
		grid_matrix* split(TPIE_OS_SIZE_T s, const point_t& p, TPIE_OS_SIZE_T d) {
			TPLOG("  ::grid_matrix::split Entering\n");
      
			assert(d < dim);
			assert(s < gt[d]);
			TPIE_OS_SIZE_T i, j, ni;
      
			// The high matrix will be returned in gmx.
			grid_matrix* gmx = new grid_matrix(*this);
			// The no. of strips is the same on all dimensions except d.
			gmx->gt[d] = gt[d] - s;
			// The grid lines are the same on all dimensions except d, where
			// we have to advance the pointer by s.
			gmx->gl[d] = gl[d] + s;
      
			// Splitting the count matrix is much trickier. All this uglyness
			// should be highly optimized by the compiler for 2 dimensions.
      
			// Multipliers for this matrix.
			size_t mult[dim+1]; // mult[i] = t[0] * .. * t[i-1]
			mult[0] = 1;
			for (i = 1; i <= dim; i++)
				mult[i] = mult[i-1] * gt[i-1];
			assert(mult[dim] == sz);
			TPLOG("    initial size: "<<sz<<"\n");
      
			// This matrix will become the low matrix. Update the number of
			// strips on dimension d. No need to update the grid lines.
			gt[d] = s + 1;
      
			// Multipliers for the low matrix.
			size_t lo_mult[dim+1];
			lo_mult[0] = 1;
			for (i = 1; i <= dim; i++)
				lo_mult[i] = lo_mult[i-1] * gt[i-1];
			// The low matrix. Will replace matrix c later, when we're done with it.
			TPIE_OS_SIZE_T* lo_c = new TPIE_OS_SIZE_T[lo_mult[dim]];
			// The new size.
			sz = lo_mult[dim];
			TPLOG("    low size: "<<sz<<"\n");
      
			// Multipliers for the high matrix.
			TPIE_OS_SIZE_T hi_mult[dim+1];
			hi_mult[0] = 1;
			for (i = 1; i <= dim; i++)
				hi_mult[i] = hi_mult[i-1] * gmx->gt[i-1];
			// The high matrix.
			gmx->c = new TPIE_OS_SIZE_T[hi_mult[dim]];
			// The size of gmx.
			gmx->sz = hi_mult[dim];
			TPLOG("    high size: "<<gmx->sz<<"\n");
      
			// i is i_0*mult[0] + i_1*mult[1] + ...
			// For each element in the matrix, decide whether it goes in the
			// low or in the high matrix.
			for (i = 0; i < mult[dim]; i++) {
				ni = 0;
	
				// Check on which side of strip s the cell given by i falls. Don't
				// need the ones on the boundary strip, since those will get new
				// values later.
				if ((i % mult[d+1]) / mult[d] < s) {
					// Compute the index in the lo_c array.
					for (j = 0; j < dim; j++)
						ni += ((i % mult[j+1]) / mult[j]) * lo_mult[j];
					// Fill in the corresponding position in lo_c.
					lo_c[ni] = c[i];
				} else if ((i % mult[d+1]) / mult[d] > s) {
					// Compute the index in the gmx->c array.
					for (j = 0; j < dim; j++)
						ni += ((i % mult[j+1]) / mult[j] - (j==d ? s: 0)) * hi_mult[j];
					// Fill in the corresponding position in the gmx->c array.
					gmx->c[ni] = c[i];
				} else { // boundary. initialize these to 0.
					// Compute the index position in the lo_c array.
					for (j = 0; j < dim; j++)
						ni += ((i % mult[j+1]) / mult[j]) * lo_mult[j];
					// Initialize to 0.
					lo_c[ni] = 0;
	  
					ni = 0;
					// Compute the index position in the gmx->c array.
					for (j = 0; j < dim; j++)
						ni += ((i % mult[j+1]) / mult[j] - (j==d ? s: 0)) * hi_mult[j];
					// Initialize to 0.
					gmx->c[ni] = 0;
				}
			}
      
			delete [] c;
			c = lo_c;
      
			err err;
			point_t* p1;
			TPIE_OS_SIZE_T i_i, m;
			TPIE_OS_OFFSET median_strip = gl[d] + s; // refers to the big grid!
			TPLOG("    median strip in grid: "<<median_strip<<"\n");
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
			hi[d] = std::pair<record<coord_t, TPIE_OS_SIZE_T, dim>, bool>(p, true);
			gmx->lo[d] = std::pair<record<coord_t, TPIE_OS_SIZE_T, dim>, bool>(p, true);
#else
			hi[d] = std::pair<coord_t, bool>(p[d], true);
			gmx->lo[d] = std::pair<coord_t, bool>(p[d], true);
#endif
			TPIE_OS_OFFSET off = g->o[d][median_strip];
			TPLOG("    stream offset of first pnt in median strip: "<<off<<"\n");
			g->streams[d]->seek(off);
      
			// Compute the counts for the cells on the rightmost strip in this
			// matrix and for the cells on the leftmost strip in the gmx matrix.
      
			while ((err = g->streams[d]->read_item(&p1)) == tpie::ami::NO_ERROR) {
				// Stop when reaching the offset of the next strip.
				if (median_strip < TPIE_OS_OFFSET(g->t[d]) - 1 && off >= TPIE_OS_OFFSET(g->o[d][median_strip + 1]))
					break;
	
				// This test is using the new values for hi.
				if (is_inside(*p1)) {
					m = 1;
					ni = 0;
					for (i = 0; i < dim; i++) {
						i_i = std::upper_bound(g->l[i] + gl[i], g->l[i] + (gl[i] + gt[i]-1), (*p1)[i]) 
							- (g->l[i] + gl[i]);
						// On dimension d, we should have s.
						assert(i != d || i_i == s);
						assert(i_i < gt[i]);
						ni += i_i * m;
						m *= gt[i];
					}
					assert(ni < sz);
					c[ni]++;
					assert(!gmx->is_inside(*p1));
				}
				if (gmx->is_inside(*p1)) {
					m = 1;
					ni = 0;
					for (i = 0; i < dim; i++) {
						i_i = std::upper_bound(g->l[i] + gmx->gl[i], g->l[i] + (gmx->gl[i] + gmx->gt[i]-1), (*p1)[i]) 
							- (g->l[i] + gmx->gl[i]);
						// On dimension d, we should have 0.
						assert(i != d || i_i == 0);
						assert(i_i < gmx->gt[i]);
						ni += i_i * m;
						m *= gmx->gt[i];
					}
					assert(ni < gmx->sz);
					gmx->c[ni]++;
					assert(!is_inside(*p1));
				}
				off++;
			}  
			TPLOG("  ::grid_matrix::split Exiting\n");
			return gmx;
		}


		///////////////////////////////////////////////////////////////////////////
		/// Finds the median point, store it in \p p, splits according to the
		/// median point, and returns the "high" sub-grid.
		///////////////////////////////////////////////////////////////////////////
		grid_matrix* find_median_and_split(point_t& p, TPIE_OS_SIZE_T d, TPIE_OS_OFFSET median_pos) {
			TPLOG("  ::grid_matrix::find_median_and_split Entering dim="<<d<<"\n");
			TPIE_OS_SIZE_T s = 0, i;
			TPIE_OS_OFFSET acc = 0;
			grid_matrix* gmx;
			err err;
      
			// Preparation: compute point counts for each strip orthogonal to d.
			TPIE_OS_OFFSET *strip_count = new TPIE_OS_OFFSET[gt[d]];
			for (i = 0; i < gt[d]; i++)
				strip_count[i] = 0;
			TPIE_OS_OFFSET mult[dim+1]; // mult[i] = t[0] * .. * t[i-1]
			mult[0] = 1;
			for (i = 1; i <= dim; i++)
				mult[i] = mult[i-1] * gt[i-1];
			for (i = 0; i < sz; i++) {
				strip_count[(i % mult[d+1]) / mult[d]] += c[i];
			}
			// Find median strip s on dimension d based on the strip counts.
			for (i = 0; i < gt[d]; i++) {
				if (acc + strip_count[i] >= median_pos + 1)
					break;
				else
					acc += strip_count[i];
			}
			assert(acc < point_count);
			assert(acc <= median_pos);
			assert(i < gt[d]);
			// Store the index of the median strip in s.
			s = i;
      
			TPIE_OS_OFFSET offset_in_strip = median_pos - acc; // refers to this subgrid.
			assert(offset_in_strip < strip_count[s]);
			delete [] strip_count;
      
			TPLOG("    median strip: "<<s<<"\n");
			TPLOG("    offset in median strip: "<<offset_in_strip<<"\n");
      
			// Find the exact median point. Tricky.
			point_t* p1, ap;
			i = 0;
			g->streams[d]->seek(g->o[d][gl[d]+s]);
			err = g->streams[d]->read_item(&p1);
      
			while (err == tpie::ami::NO_ERROR) {
				assert((*p1)[d] >= g->l[d][gl[d]+(s-1)]);
				assert((*p1)[d] <  g->l[d][gl[d]+s]);
	
				if (is_inside(*p1)) {
					if ((TPIE_OS_OFFSET)i == offset_in_strip) {
						ap = *p1;
						break;
					}
					// Count only points inside.
					i++;
				}
				err = g->streams[d]->read_item(&p1);
			}
      
			assert((TPIE_OS_OFFSET)i == offset_in_strip);
			TPLOG("    preliminary median point: ("<<ap[0]<<","<<ap[1]<<")\n");
			assert(err == tpie::ami::NO_ERROR);
      
			err = g->streams[d]->read_item(&p1);
      
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
			while (err == tpie::ami::NO_ERROR && ap == (*p1)) {
#else
				// Keep reading until the coordinate on dimension d is different.
				while (err == tpie::ami::NO_ERROR && ap[d] == (*p1)[d]) {
#endif
					if (is_inside(*p1)) {
						offset_in_strip++;
						ap = *p1;
						TPLOG("    advanced offset_in_strip. new point: ("<<ap[0]<<","<<ap[1]<<")\n");
					}
					err = g->streams[d]->read_item(&p1);
				}
      
				// The point we are looking for is ap.
      
				TPLOG("    new offset in median strip: "<<offset_in_strip<<"\n");
				TPLOG("    final median point: ("<<ap[0]<<","<<ap[1]<<")\n");
      
				// Split the matrix.
				gmx = split(s, ap, d);
      
				// This matrix is now the low matrix and gmx is the high matrix.
				assert(is_inside(ap));
      
				// Update point_count for the high matrix.
				gmx->point_count = point_count - (offset_in_strip + acc + 1);
				TPLOG("    high matrix point count: "<<gmx->point_count<<"\n");
      
				// Update point_count for the low matrix.
				point_count = offset_in_strip + acc + 1;
				TPLOG("    low matrix point count: "<<point_count<<"\n");
      
				p = ap;
      
				TPLOG("  ::grid_matrix::find_median_and_split Exiting p=("<<p[0]<<","<<p[1]<<")\n");
				// Return.
				return gmx;
			}
      

			// Return true if given point is inside the box defined by hi and lo.
			bool is_inside(const point_t& p) {
				bool ans = true;
				TPIE_OS_SIZE_T i;
				for (i = 0; i < dim; i++) {
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
					if (lo[i].second && typename point_t::cmp(i).compare(p, lo[i].first) <= 0) {
						ans = false;
						break;
					} else if (hi[i].second && typename point_t::cmp(i).compare(p, hi[i].first) > 0) {
						ans = false;
						break;
					}
#else
					if (lo[i].second && p[i] <= lo[i].first) {
						ans = false;
						break;
					} else if (hi[i].second && p[i] > hi[i].first) {
						ans = false;
						break;
					}
#endif
				}
				return ans;
			}

			// Destructor. Delete c.
			~grid_matrix() {
				delete [] c;
			}
		};

		class grid_context {
		public:
			bid_t bid;
			bn_context ctx;
			stream_t* streams[dim];
			std::string stream_names[dim];
			bool low;

#define NEW_DISTRIBUTE_G 1
#if NEW_DISTRIBUTE_G
			grid_matrix gmx;
#endif

			grid_context(bid_t _bid, bn_context _ctx, bool _low, 
						 const grid_matrix& _gmx):
				bid(_bid), ctx(_ctx), low(_low), gmx(_gmx) {}
		};
  
		///////////////////////////////////////////////////////////////////////////
		/// The grid info for the new bulk loading alg.
		///////////////////////////////////////////////////////////////////////////
		class grid {
		public:
			/** The number of strips on each dimension. To avoid confusion: the
			 * tic-tac-toe board has t[0]=t[1]=3. There should be at least 2
			 * strips on each dimension. The leftmost and rightmost strips on
			 * each dimension are unbounded. */
			TPIE_OS_SIZE_T t[dim];
			/** Pointer to an array of dim streams containing the points. These
			 * are neither initialized nor destroyed here. */
			stream_t** streams;
			/** The coordinates of the grid lines. l[i] is an array of length
			 *  t[i]-1. */ 
			coord_t* l[dim]; 
			/** o[i][j] is the offset in streams[i] of the point that defines
			 * grid line l[i][j-1]. o[i] is an array of length t[i]. */
			TPIE_OS_OFFSET *o[dim];
			TPIE_OS_OFFSET point_count;
			/** The queue of unfinished business. */
			std::vector<grid_context> q;

			///////////////////////////////////////////////////////////////////////////
			/// Constructor.
			///////////////////////////////////////////////////////////////////////////
			grid(TPIE_OS_SIZE_T t_all, stream_t** in_streams) {

				streams = in_streams;
      
				point_count = in_streams[0]->stream_len();
				TPIE_OS_SIZE_T i, j;
				TPIE_OS_OFFSET off;
				record<coord_t, size_t, dim> *p1, ap;
				err err;
      
				// Determine the grid lines.
				for (i = 0; i < dim; i++) {
					t[i] = t_all;
					l[i] = new coord_t[t[i] - 1];
					o[i] = new TPIE_OS_OFFSET[t[i]];
					assert((TPIE_OS_SIZE_T)point_count > 2 * t[i]); // TODO: make this more meaningful.
					o[i][0] = 0;
					for (j = 0; j < t[i]-1; j++) {
						off = (j + 1) * (point_count / t[i]) - 1;
						in_streams[i]->seek(off);
						err = in_streams[i]->read_item(&p1);
						assert(err == tpie::ami::NO_ERROR);
						ap = *p1;
						err = in_streams[i]->read_item(&p1);
						while (err == tpie::ami::NO_ERROR && (*p1)[i] == ap[i]) {
							ap = *p1;
							err = in_streams[i]->read_item(&p1);
							off++;
						}
						assert(err == tpie::ami::NO_ERROR);
						// The first point with a different value on the i'th dimension.
						l[i][j] = (*p1)[i];
						o[i][j+1] = off + 1;
					}
				}
			}


			///////////////////////////////////////////////////////////////////////////
			/// Destructor.
			///////////////////////////////////////////////////////////////////////////
			~grid() {
				size_t i;
				for (i = 0; i < dim; i++) {
					delete [] l[i];
					delete [] o[i];
				}
				q.clear();
				//  assert(q.size() == 0);
			}


			grid_matrix* create_matrix() {
      
				size_t i, j;
				err err;
      
				size_t len, half;
				coord_t* middle, *first, val;
      
				grid_matrix* gmx = new grid_matrix(t, this);
      
				gmx->c = new TPIE_OS_SIZE_T[gmx->sz];
				for (j = 0; j < gmx->sz; j++)
					gmx->c[j] = 0;
      
				size_t mult = 1; // multiplier.
				size_t ni = 0, i_i, i_0 = 0;
				point_t* p2;
				coord_t oldvalue;
      
				streams[0]->seek(0);
				err = streams[0]->read_item(&p2);
				oldvalue = (*p2)[0];
      
				// Compute the counts. Loop over all points.
				while (err == tpie::ami::NO_ERROR) {
	
					// Since streams[0] is sorted on the first dimension, there's no
					// need for binary search on this dimension.
					if (i_0 < t[0] - 1)
						if (l[0][i_0] == (*p2)[0] && (*p2)[0] > oldvalue) {
							i_0++;
							oldvalue = (*p2)[0];
						}
					ni = i_0;
					///    mult = t[0];
					mult = 1;
	
					for (i = 1; i < dim; i++) {
						val = (*p2)[i];
						mult *= t[i-1]; ///
						// Do binary search with (*p2)[i] over the lines in l[i] to
						// find the i'th coordinate.
	  
						///      i_i = upper_bound(l[i], l[i] + (t[i]-1), (*p2)[i]) - l[i];
						// START New code, taken from the stl library (stl_algo.h).
						len = t[i]-1;
						first = l[i];
						while (len > 0) {
							half = len >> 1;
							middle = first + half;
							if (val < *middle)
								len = half;
							else {
								first = middle + 1;
								len -= half + 1;
							}
						}
						i_i = first - l[i];
						// END New code.
	  
						assert(i_i < t[i]);
						ni += i_i * mult;
						///      mult *= t[i];
					}
					//    assert(ni < gmx->sz);
					gmx->c[ni]++;
					err = streams[0]->read_item(&p2);
				}
				return gmx;
			}

		};


		///////////////////////////////////////////////////////////////////////////
		/// Used by the sample bulk loader. Similar to grid_context. 
		///////////////////////////////////////////////////////////////////////////
		class sample_context {
		public:
			bid_t bid;
			bn_context ctx;
			bool low;
			stream_t* stream;
			std::string stream_name;

			sample_context(bid_t _bid, bn_context _ctx, bool _low):
				bid(_bid), ctx(_ctx), low(_low) {}
		};

		///////////////////////////////////////////////////////////////////////////
		/// Sample info for the sample-based bulk loader.
		///////////////////////////////////////////////////////////////////////////
		class sample {
		public:
			// Pointer to the input stream.
			stream_t* in_stream;
			// The in-memory streams containing the sampled points.
			point_t* mm_streams[dim];
			// The number of sample points.
			TPIE_OS_SIZE_T sz;
			// The queue of unfinished business.
			std::vector<sample_context> q;

			// Construct a sample.
			sample(TPIE_OS_SIZE_T _sz, stream_t* _in_stream) {
#define MAX_RANDOM ((double)0x7fffffff)

				// Preliminary sample size.
				sz = _sz;
				in_stream = _in_stream;
				TPIE_OS_OFFSET input_sz = in_stream->stream_len();
				assert(sz > 0 && (TPIE_OS_OFFSET)sz < input_sz);
      
				TPIE_OS_OFFSET* offsets = new TPIE_OS_OFFSET[sz];
				TPIE_OS_OFFSET* new_last;
				point_t *p;
				TPIE_OS_SIZE_T i;

				TPIE_OS_SRANDOM(10);
      
				// Sample sz offsets in the interval [0, input_sz].
				for (i = 0; i < sz; i++) {
					offsets[i] = TPIE_OS_OFFSET((TPIE_OS_RANDOM()/MAX_RANDOM) * input_sz);
				}
      
				// Sort the sampled offsets.
				std::sort(offsets, offsets + sz);
      
				// Eliminate duplicates.
				if ((new_last = std::unique(offsets, offsets + sz)) != offsets + sz) {
					std::cerr << "    Warning: Duplicate samples found! Decreasing sample size accordingly.\n";
					// Adjust sample size sz.
					sz = new_last - offsets;
					std::cerr << "    New sample size: " << (TPIE_OS_OFFSET)sz << "\n";
				}
      
				// Make space for one in-memory array (more later).
				mm_streams[0] = new point_t[sz];
      
				// Read the sample points.
				for (i = 0; i < sz; i++) {
					assert(offsets[i] < input_sz);
					in_stream->seek(offsets[i]);
					in_stream->read_item(&p);
					mm_streams[0][i] = *p;
				}
      
				// Delete the offsets array.
				delete [] offsets;
      
				// Make space for the other (d-1) in-memory arrays and copy the
				// points from the existing array.
				for (i = 1; i < dim; i++) {
					mm_streams[i] = new point_t[sz];
					for (size_t j = 0; j < sz; j ++)
						mm_streams[i][j] = mm_streams[0][j];
				}
      
				// Sort the d in-memory arrays on each dimension.
				typename point_t::cmp* comp_obj;
				for (i = 0; i < dim; i++) {
					comp_obj = new typename point_t::cmp(i);
					std::sort(mm_streams[i], mm_streams[i]+sz, *comp_obj);
					delete comp_obj;
				}
			}
    
			// Remove the sample points.
			void cleanup() {
				size_t i;
				for (i = 0; i < dim; i++) {
					delete [] mm_streams[i];
					mm_streams[i] = NULL;
				}
			}

			~sample() {
				cleanup();
			}
		};

		///////////////////////////////////////////////////////////////////////////
		/// Pair of dim flags. Used in window_query.
		///////////////////////////////////////////////////////////////////////////
		struct podf {
			bool first[dim];
			bool second[dim];
			bool alltrue() {
				for (size_t i = 0; i < dim; i++) {
					if (!first[i] || !second[i])
						return false;
				}
				return true;
			}
		};

		typedef std::pair<podf, std::pair<bid_t,link_type_t> > outer_stack_elem;
		typedef std::pair<podf, size_t>                   inner_stack_elem;

		///////////////////////////////////////////////////////////////////////////
		/// Used for printing the binary kd-tree.
		/// An element represents a binary kd-tree
		/// node and the number of times it was visited. 
		/// bid_t id the block id of the block node, and idx is the index
		/// of the bin node (or -1 for a leaf node).
		/// lo and hi form the mbr of the node.
		///////////////////////////////////////////////////////////////////////////
		struct print_stack_elem {
			bid_t bid;
			int idx;
			int visits;
			point_t lo;
			point_t hi;
			print_stack_elem(bid_t _bid, int _idx, int _visits, point_t _lo, point_t _hi): bid(_bid), idx(_idx), visits(_visits), lo(_lo), hi(_hi) {}
		};

		///////////////////////////////////////////////////////////////////////////
		/// Used for nearest neighbor searching.
		///////////////////////////////////////////////////////////////////////////
		struct nn_pq_elem {
			double p; // the priority (the distance squared)
			bid_t bid;
			link_type_t type;
		};

		///////////////////////////////////////////////////////////////////////////
		/// Helper for binary distribution bulk loading.
		///////////////////////////////////////////////////////////////////////////
		void create_bin_node(node_t *b, bn_context ctx, 
							 stream_t** in_streams, 
							 size_t& next_free_el, size_t& next_free_lk);
		///////////////////////////////////////////////////////////////////////////
		/// Helper for binary distribution bulk loading.
		///////////////////////////////////////////////////////////////////////////
		void create_node(bid_t& bid, TPIE_OS_SIZE_T d,
						 stream_t** in_streams);
		///////////////////////////////////////////////////////////////////////////
		/// Helper for binary distribution bulk loading.
		///////////////////////////////////////////////////////////////////////////
		void create_leaf(bid_t& bid, TPIE_OS_SIZE_T d,
						 stream_t** in_streams);

		///////////////////////////////////////////////////////////////////////////
		/// Helper for in-memory bulk loading.
		///////////////////////////////////////////////////////////////////////////
		bool can_do_mm(TPIE_OS_OFFSET sz);
 
		///////////////////////////////////////////////////////////////////////////
		/// Copies the given sorted streams into newly created in-memory
		/// streams. The input streams are deleted.
		///////////////////////////////////////////////////////////////////////////
		void copy_to_mm(stream_t** in_streams, 
						point_t** mm_streams, TPIE_OS_SIZE_T& sz);

		///////////////////////////////////////////////////////////////////////////
		/// Copies the given stream in memory, makes \p dim copies of it, and sorts
		/// them on the \p dim dimensions.
		///////////////////////////////////////////////////////////////////////////
		void copy_to_mm(stream_t* in_stream, 
						point_t** mm_streams, TPIE_OS_SIZE_T& sz);
		void create_bin_node_mm(node_t *b, bn_context ctx, 
								point_t** in_streams, TPIE_OS_SIZE_T sz,
								size_t& next_free_el, size_t& next_free_lk);
		void create_node_mm(bid_t& bid, TPIE_OS_SIZE_T d,
							point_t** in_streams, TPIE_OS_SIZE_T sz);
		void create_leaf_mm(bid_t& bid, TPIE_OS_SIZE_T d,
							point_t** in_streams, TPIE_OS_SIZE_T sz);

		// Helpers for grid-based bulk loading.
		void create_bin_node_g(node_t *b, bn_context ctx, 
							   grid_matrix *gmx, size_t& next_free_el, size_t& next_free_lk);
		void create_node_g(bid_t& bid, TPIE_OS_SIZE_T d, grid_matrix* gmx);
		void create_grid(bid_t& bid, TPIE_OS_SIZE_T d, stream_t** in_streams, TPIE_OS_SIZE_T t);
		void distribute_g(bid_t bid, TPIE_OS_SIZE_T d, grid* g);
		void build_lower_tree_g(grid* g);

		bool points_are_sample;
		/** Global sample object.*/
		sample* gso;
		// Helpers for sample-based bulk loading. 
		void create_sample(bid_t& bid, TPIE_OS_SIZE_T d, stream_t* in_stream);
		void distribute_s(bid_t bid, TPIE_OS_SIZE_T d, sample* s);
		void build_lower_tree_s(sample* s);

		///////////////////////////////////////////////////////////////////////////
		/// Finds the leaf where \p p might be.
		///////////////////////////////////////////////////////////////////////////
		bid_t find_leaf(const point_t &p);

		///////////////////////////////////////////////////////////////////////////
		/// Fetches a node from cache or disk. If bid_t is 0, a new node is created.
		///////////////////////////////////////////////////////////////////////////
		node_t* fetch_node(bid_t bid = 0);

		///////////////////////////////////////////////////////////////////////////
		/// Fetches a leaf.
		///////////////////////////////////////////////////////////////////////////
		leaf_t* fetch_leaf(bid_t bid = 0);

		///////////////////////////////////////////////////////////////////////////
		/// Releases a node (put it into the cache).
		///////////////////////////////////////////////////////////////////////////
		void release_node(node_t* q);

		///////////////////////////////////////////////////////////////////////////
		/// Releases a leaf.
		///////////////////////////////////////////////////////////////////////////
		void release_leaf(leaf_t* q);
	};


	struct _kdtree_leaf_info {
		TPIE_OS_SIZE_T size;
		bid_t next;
#if TPIE_AMI_KDTREE_USE_KDBTREE_LEAF
		TPIE_OS_SIZE_T split_dim;
#endif
	};

///////////////////////////////////////////////////////////////////////////
/// A kdtree leaf is a block of point's. The info field contains the
/// number of points actually stored (ie, the size) and the id of
/// another leaf. All leaves in a tree are threaded this way.
///////////////////////////////////////////////////////////////////////////
	template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL> 
	class kdtree_leaf: public block<record<coord_t, TPIE_OS_SIZE_T, dim>, _kdtree_leaf_info, BTECOLL> 
	{
	public:
		using block<record<coord_t, TPIE_OS_SIZE_T, dim>, _kdtree_leaf_info, BTECOLL>::info;
		using block<record<coord_t, TPIE_OS_SIZE_T, dim>, _kdtree_leaf_info, BTECOLL>::el;
		using block<record<coord_t, TPIE_OS_SIZE_T, dim>, _kdtree_leaf_info, BTECOLL>::dirty;
  
		typedef record<coord_t, TPIE_OS_SIZE_T, dim> point_t;
		typedef stream<point_t> stream_t;
		typedef collection_single<BTECOLL> collection_t;
		typedef _kdtree_leaf_info info_t;

		static TPIE_OS_SIZE_T el_capacity(TPIE_OS_SIZE_T block_size);

		kdtree_leaf(collection_t* pcoll, bid_t bid = 0);

		///////////////////////////////////////////////////////////////////////////
		/// Returns the number of points stored in this leaf.
		///////////////////////////////////////////////////////////////////////////
		TPIE_OS_SIZE_T& size() { return info()->size; }
		const TPIE_OS_SIZE_T& size() const { return info()->size; }

		///////////////////////////////////////////////////////////////////////////
		// Returns the weight of a leaf, being its size. Just for symmetry with the
		// nodes.
		///////////////////////////////////////////////////////////////////////////
		TPIE_OS_OFFSET weight() const { return info()->size; }

		///////////////////////////////////////////////////////////////////////////
		// Returns the next leaf. All leaves of a tree are chained together for easy
		// retrieval.
		///////////////////////////////////////////////////////////////////////////
		const bid_t& next() const { return info()->next; }
		bid_t& next() { return info()->next; }

#if TPIE_AMI_KDTREE_USE_KDBTREE_LEAF
		TPIE_OS_SIZE_T& split_dim() { return info()->split_dim; }
		const TPIE_OS_SIZE_T& split_dim() const { return info()->split_dim; }
#endif 

		///////////////////////////////////////////////////////////////////////////
		/// Returns the maximum number of points that can be stored in this leaf.
		///////////////////////////////////////////////////////////////////////////
		TPIE_OS_SIZE_T capacity() const { return el.capacity(); }

		///////////////////////////////////////////////////////////////////////////
		/// Finds a point. Returns the index of the point found in the el
		/// vector (if not found, return size()).
		///////////////////////////////////////////////////////////////////////////
		TPIE_OS_SIZE_T find(const point_t &p) const;

		///////////////////////////////////////////////////////////////////////////
		/// Performs a window_query, defined by points \p lop and \p hip.
		/// The result is written to \p stream.
		///////////////////////////////////////////////////////////////////////////
		TPIE_OS_OFFSET window_query(const point_t &lop, 
									const point_t &hip, 
									stream_t* stream) const;

		///////////////////////////////////////////////////////////////////////////
		/// Inserts a point, assuming the leaf is not full.
		///////////////////////////////////////////////////////////////////////////
		bool insert(const point_t &p);

		bool erase(const point_t &p);

		///////////////////////////////////////////////////////////////////////////
		// Sorts points on the given dimension.
		///////////////////////////////////////////////////////////////////////////
		void sort(TPIE_OS_SIZE_T d);

		///////////////////////////////////////////////////////////////////////////
		// Finds median point on the given dimension. Returns the index of the
		// median in the el vector.
		///////////////////////////////////////////////////////////////////////////
		TPIE_OS_SIZE_T find_median(TPIE_OS_SIZE_T d);
	};


	struct _kdtree_node_info {
		TPIE_OS_SIZE_T size;
		TPIE_OS_OFFSET weight;
	};

///////////////////////////////////////////////////////////////////////////
/// A kdtree node is a block of binary kdtree nodes (of templated type
/// Bin_node). The info field contains the number of Bin_node's
/// actually stored and the weight of the node (ie, the number of
/// points stored in the subtree rooted on this node).
///////////////////////////////////////////////////////////////////////////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	class kdtree_node: public block<Bin_node, _kdtree_node_info, BTECOLL> {
	public:
		using block<Bin_node, _kdtree_node_info, BTECOLL>::info;
		using block<Bin_node, _kdtree_node_info, BTECOLL>::el;
		using block<Bin_node, _kdtree_node_info, BTECOLL>::lk;
		using block<Bin_node, _kdtree_node_info, BTECOLL>::dirty;
  
		typedef record<coord_t, TPIE_OS_SIZE_T, dim> point_t;
		typedef stream<point_t> stream_t;
		typedef collection_single<BTECOLL> collection_t;

		///////////////////////////////////////////////////////////////////////////
		/// Computes the capacity of the lk vector STATICALLY (but you have to
		/// give it the correct logical block size!).
		///////////////////////////////////////////////////////////////////////////
		static TPIE_OS_SIZE_T lk_capacity(TPIE_OS_SIZE_T block_size);

		///////////////////////////////////////////////////////////////////////////
		/// Computes the capacity of the el vector STATICALLY.
		///////////////////////////////////////////////////////////////////////////
		static TPIE_OS_SIZE_T el_capacity(TPIE_OS_SIZE_T block_size);

		kdtree_node(collection_t* pcoll, bid_t bid = 0);

		///////////////////////////////////////////////////////////////////////////
		/// Returns the number of binary nodes stored in this node.
		///////////////////////////////////////////////////////////////////////////
		TPIE_OS_SIZE_T& size() { return info()->size; }
		const TPIE_OS_SIZE_T& size() const { return info()->size; }

		TPIE_OS_OFFSET& weight() { return info()->weight; }
		TPIE_OS_OFFSET weight() const { return info()->weight; }

		///////////////////////////////////////////////////////////////////////////
		/// Returns the maximum number of binary nodes that can be stored in this node.
		///////////////////////////////////////////////////////////////////////////
		TPIE_OS_SIZE_T capacity() const { return el.capacity(); }

		///////////////////////////////////////////////////////////////////////////
		/// Finds the child node that leads to \p p. The second
		/// entry in the pair tells whether that pointer is a leaf bid
		/// or a node bid.
		///////////////////////////////////////////////////////////////////////////
		std::pair<bid_t, link_type_t> find(const point_t &p) const;

		std::pair<TPIE_OS_SIZE_T, link_type_t> find_index(const point_t &p) const;
	};


////////////////////////////////////////////////////////////
///////////////     ***Implementation***    ////////////////
////////////////////////////////////////////////////////////

#define DBG(msg)      std::cerr << msg << std::flush

// Shortcuts for my convenience.
#define TPIE_AMI_KDTREE_LEAF  kdtree_leaf<coord_t, dim, BTECOLL>
#define TPIE_AMI_KDTREE_NODE  kdtree_node<coord_t, dim, Bin_node, BTECOLL>
#define TPIE_AMI_KDTREE       kdtree<coord_t, dim, Bin_node, BTECOLL>
#define POINT            record<coord_t, TPIE_OS_SIZE_T, dim>
#define POINT_STREAM     stream< POINT >

#define MEM_AVAIL 
//((float)(MM_manager.memory_available() /(MM_manager.memory_limit()/1000)) / 10)
#define MEMDISPLAY_INIT  
//cout<<"Avail. memory:      "<<flush;
#define MEMDISPLAY 
//cout<<"\b\b\b\b\b"; std::cout.setf(ios::showpoint); std::cout.setf(ios::fixed);cout.width(4); std::cout.precision(1); std::cout<<MEM_AVAIL<<"%"<<flush;
#define MEMDISPLAY_DONE 
//cout << "\n";

#define HEIGHTDISPLAY_INIT 
//cout<<"        1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 \n"<<"Depth: "<<flush;
#define HEIGHTDISPLAY_IN(s) 
//cout<<s<<flush;
#define HEIGHTDISPLAY_OUT 
//cout <<"\b\b\b   \b\b\b"<<flush;
#define HEIGHTDISPLAY_DONE 
//cout <<"\n";


////////////////////////////////
//////// **kdtree_leaf** ///////
////////////////////////////////

//// *kdtree_leaf::el_capacity* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL>
	TPIE_OS_SIZE_T TPIE_AMI_KDTREE_LEAF::el_capacity(TPIE_OS_SIZE_T block_size) {
		return block<record<coord_t, TPIE_OS_SIZE_T, dim>, _kdtree_leaf_info, BTECOLL>::el_capacity(block_size, 0);
	}

//// *kdtree_leaf::kdtree_leaf* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL>
	TPIE_AMI_KDTREE_LEAF::kdtree_leaf(collection_single<BTECOLL>* pcoll, bid_t bid):
		block<POINT, _kdtree_leaf_info, BTECOLL>(pcoll, 0, bid) {
		TPLOG("kdtree_leaf::kdtree_leaf Entering bid="<<bid<<"\n");
#if TPIE_AMI_KDTREE_USE_KDBTREE_LEAF
		split_dim() = 0;
#endif
		TPLOG("kdtree_leaf::kdtree_leaf Exiting bid="<<bid_<<"\n");
	}

//// *kdtree_leaf::find* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL>
	TPIE_OS_SIZE_T TPIE_AMI_KDTREE_LEAF::find(const POINT &p) const {
		TPLOG("kdtree_leaf::find Entering bid="<<bid()<<" size="<<size()<<"\n");
		TPIE_OS_SIZE_T i = 0;
		while (i < size()) {
			if (p == el[i])
				break;
			i++;
		}
		TPLOG("kdtree_leaf::find Exiting ans="<<(i<size())<<"\n");
		return i; 
	}

//// *kdtree_leaf::insert* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL>
	bool TPIE_AMI_KDTREE_LEAF::insert(const POINT &p) {
		TPLOG("kdtree_leaf::insert Entering "<<"\n");
		assert(size() < el.capacity());
		// TODO: do a find() here to check for duplicate point. 
		el[size()] = p;
		size()++;
		dirty() = 1;
		TPLOG("kdtree_leaf::insert Exiting "<<"\n");
		return true;
	}

//// *kdtree_leaf::erase* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL>
	bool TPIE_AMI_KDTREE_LEAF::erase(const POINT &p) {
		TPLOG("kdtree_leaf::erase Entering "<<"\n");
		bool ans = false;
		TPIE_OS_SIZE_T idx;
		if ((idx = find(p)) < size()) {
			if (idx < size() - 1) {
				// Copy the last item indo pos idx. We could use el.erase() as
				// well, but that's slower. Here order is not important.
				el[idx] = el[size()-1];
			}
			size()--;
			ans = true;
			dirty() = 1;
		}

		TPLOG("kdtree_leaf::erase Exiting ans="<<ans<<"\n");
		return ans;
	}

//// *kdtree_leaf::window_query* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL>
	TPIE_OS_OFFSET TPIE_AMI_KDTREE_LEAF::window_query(const POINT &lop, const POINT &hip, 
													  POINT_STREAM* stream) const {
		TPLOG("kdtree_leaf::window_query Entering "<<"\n");
		TPIE_OS_SIZE_T i;
		TPIE_OS_OFFSET result = 0;
		for (i = 0; i < size(); i++) {
			// Test on all dimensions.
			if (lop < el[i] && el[i] < hip) {
				result++;
				if (stream != NULL)
					stream->write_item(el[i]);
			}
		}

		TPLOG("kdtree_leaf::window_query Exiting count="<<result<<"\n");
		return result;
	}

//// *kdtree_leaf::sort* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL>
	void TPIE_AMI_KDTREE_LEAF::sort(TPIE_OS_SIZE_T d) {
		typename POINT::cmp cmpd(d);
		std::sort(&el[0], &el[0] + size(), cmpd);
	}

//// *kdtree_leaf::find_median* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class BTECOLL>
	size_t TPIE_AMI_KDTREE_LEAF::find_median(TPIE_OS_SIZE_T d) {
		typename POINT::cmp cmpd(d);
		sort(d);
		size_t ans = (size() - 1) / 2; // preliminary median.
		while ((ans+1 < size()) && cmpd.compare(el[ans], el[ans+1]) == 0)
			ans++;
		return ans;
	}


////////////////////////////////
//////// **kdtree_node** ///////
////////////////////////////////

//// *kdtree_node::lk_capacity* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	size_t TPIE_AMI_KDTREE_NODE::lk_capacity(size_t block_size) {
		return (size_t) ((block_size - sizeof(std::pair<size_t, size_t>) - 
						  sizeof(bid_t)) / 
						 (sizeof(Bin_node) + sizeof(bid_t)) + 1);
	}

//// *kdtree_node::el_capacity* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	size_t TPIE_AMI_KDTREE_NODE::el_capacity(size_t block_size) {
		TPLOG("kdtree_node::el_capacity Entering\n");
		TPLOG("  block<Bin_node, _kdtree_node_info>::el_capacity(block_size, lk_capacity(block_size))="<<(block<Bin_node, _kdtree_node_info, BTECOLL>::el_capacity(block_size, lk_capacity(block_size)))<<"\n");
		TPLOG("  (lk_capacity(block_size) - 1)="<<(lk_capacity(block_size) - 1)<<"\n");
		// Sanity check. Two different methods of computing the el capacity.
		// [12/21/01: changed == into >= since I could fit one more element, but not one more link] 
		assert((block<Bin_node, _kdtree_node_info, BTECOLL>::el_capacity(block_size, lk_capacity(block_size))) >= (size_t) (lk_capacity(block_size) - 1));
		TPLOG("kdtree_node::el_capacity Exiting\n");
		return (size_t) (lk_capacity(block_size) - 1);
	}

//// *kdtree_node::kdtree_node* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	TPIE_AMI_KDTREE_NODE::kdtree_node(collection_single<BTECOLL>* pcoll, bid_t bid):
		block<Bin_node, _kdtree_node_info, BTECOLL>(pcoll, 
													lk_capacity(pcoll->block_size()), bid) {
		TPLOG("kdtree_node::kdtree_node Entering bid="<<bid<<"\n");
		TPLOG("kdtree_node::kdtree_node Exiting bid="<<bid_<<"\n");
	}

//// *kdtree_node::find_index* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	inline std::pair<TPIE_OS_SIZE_T, link_type_t> TPIE_AMI_KDTREE_NODE::find_index(const POINT &p) const {
		TPLOG("kdtree_node::find_index Entering "<<"\n");
		TPIE_OS_SIZE_T idx1 = 0, idx2; // the root bin node is always in pos 0.
		link_type_t idx_type = BIN_NODE;
		while (idx_type == BIN_NODE) {
			//    assert(idx1 < el.capacity());
			if (el[idx1].discriminate(p.key) <= 0)
				el[idx1].get_low_child(idx2, idx_type);
			else
				el[idx1].get_high_child(idx2, idx_type);
			// Make sure we don't loop forever.
			//    assert(idx_type != BIN_NODE || idx2 != idx1);
			idx1 = idx2;
		}
		TPLOG("kdtree_node::find_index Exiting "<<"\n");
		return std::pair<TPIE_OS_SIZE_T, link_type_t>(idx1, idx_type);
	}

//// *kdtree_node::find* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	std::pair<bid_t, link_type_t> TPIE_AMI_KDTREE_NODE::find(const POINT &p) const {
		TPLOG("kdtree_node::find Entering bid="<<bid()<<"\n");
		std::pair<size_t, link_type_t> ans = find_index(p);
		//  assert(ans.second != GRID_INDEX);
		TPLOG("kdtree_node::find Exiting "<<"\n");
		return std::pair<bid_t, link_type_t>(lk[ans.first], ans.second);
	}


//////////////////////////////////////
///////////// **kdtree** /////////////
//////////////////////////////////////


//// *kdtree::kdtree* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	TPIE_AMI_KDTREE::kdtree(const kdtree_params& params) 
		: header_(), params_(params), points_are_sample(false) {
		TPLOG("kdtree::kdtree Entering\n");

		std::string base_file_name = tempname::tpie_name("kdtree");
		name_ = base_file_name;
		shared_init(base_file_name, AMI_WRITE_COLLECTION);
		if (status_ == KDTREE_STATUS_VALID) {
			persist(PERSIST_DELETE);
		}

		TPLOG("kdtree::kdtree Exiting status="<<status_<<", size="<<header_.size<<"\n");
	}


//// *kdtree::kdtree* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	TPIE_AMI_KDTREE::kdtree(const std::string& base_file_name, collection_type type, 
							const kdtree_params& params) 
		: header_(), params_(params), name_(base_file_name), points_are_sample(false) {
		TPLOG("kdtree::kdtree Entering base_file_name="<<base_file_name<<"\n");

		shared_init(base_file_name, type);

		TPLOG("kdtree::kdtree Exiting status="<<status_<<", size="<<header_.size<<"\n");
	}

//// *kdtree::shared_init* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::shared_init(const std::string& base_file_name, collection_type type) {
		TPLOG("kdtree::shared_init Entering "<<"\n");

		status_ = KDTREE_STATUS_VALID;


		std::string collname = base_file_name;

		// Open the two block collections.
		pcoll_leaves_ = new collection_t(collname+".l", type, params_.leaf_block_factor);
		pcoll_nodes_ = new collection_t(collname+".n", type, params_.node_block_factor);

		if (!pcoll_nodes_->is_valid() || !pcoll_leaves_->is_valid()) {
			status_ = KDTREE_STATUS_INVALID;
			delete pcoll_nodes_;
			delete pcoll_leaves_;
			return;
		}

		// Read the header info, if relevant.
		if (pcoll_leaves_->size() != 0) {
			//    header_ = *((header_t *) pcoll_nodes_->user_data());
			memcpy((void *)(&header_), pcoll_nodes_->user_data(), sizeof(header_));
			if (header_.magic_number != TPIE_AMI_KDTREE_HEADER_MAGIC_NUMBER) {
				status_ = KDTREE_STATUS_INVALID;
				TP_LOG_WARNING_ID("Invalid magic number in kdtree file.");
				delete pcoll_nodes_;
				delete pcoll_leaves_;
				return;
			}
			if (header_.store_weights != TPIE_AMI_KDTREE_STORE_WEIGHTS) {
				status_ = KDTREE_STATUS_INVALID;
				TP_LOG_WARNING_ID("Invalid kdtree. Mismatch for TPIE_AMI_KDTREE_STORE_WEIGHTS.");
				delete pcoll_nodes_;
				delete pcoll_leaves_;
				return;
			}
			if (header_.use_exact_split != TPIE_AMI_KDTREE_USE_EXACT_SPLIT) {
				status_ = KDTREE_STATUS_INVALID;
				TP_LOG_WARNING_ID("Invalid kdtree. Mismatch for TPIE_AMI_KDTREE_USE_EXACT_SPLIT.");
				delete pcoll_nodes_;
				delete pcoll_leaves_;
				return;
			}
			if (header_.use_kdbtree_leaf != TPIE_AMI_KDTREE_USE_KDBTREE_LEAF) {
				status_ = KDTREE_STATUS_INVALID;
				TP_LOG_WARNING_ID("Invalid kdtree. Mismatch for TPIE_AMI_KDTREE_USE_KDBTREE_LEAF.");
				delete pcoll_nodes_;
				delete pcoll_leaves_;
				return;
			}
			if (header_.use_real_median != TPIE_AMI_KDTREE_USE_REAL_MEDIAN) {
				TP_LOG_WARNING_ID("Warning: Mismatch for TPIE_AMI_KDTREE_USE_REAL_MEDIAN");
			}
			// TODO: more sanity checks on the header.
		}

		// Initialize the caches.
		leaf_cache_ = new leaf_cache_t(params_.leaf_cache_size, 8);
		node_cache_ = new node_cache_t(params_.node_cache_size, 8);

		// Give meaningful values to parameters, if necessary.
		size_t leaf_capacity = TPIE_AMI_KDTREE_LEAF::el_capacity(pcoll_leaves_->block_size());
		if (params_.leaf_size_max == 0 || params_.leaf_size_max > leaf_capacity)
			params_.leaf_size_max = leaf_capacity;
		TPLOG("  leaf_size_max="<<params_.leaf_size_max<<"\n");

		size_t node_capacity = TPIE_AMI_KDTREE_NODE::el_capacity(pcoll_nodes_->block_size());
		if (params_.node_size_max == 0 || params_.node_size_max > node_capacity)
			params_.node_size_max = node_capacity;
		TPLOG("  node_size_max="<<params_.node_size_max<<"\n");

		size_t i;

		// Initialize params_.max_intranode_height
		if (params_.max_intranode_height == 0) {
			for (i = 0; i < 64; i++)
				if (params_.node_size_max >> i == 1)
					break;
			assert(i < 64);
			params_.max_intranode_height = i + 1;
		}
		TPLOG("  max_intranode_height="<<params_.max_intranode_height<<"\n");

		if (params_.max_intraroot_height == 0)
			params_.max_intraroot_height = params_.max_intranode_height;
		TPLOG("  max_intraroot_height="<<params_.max_intraroot_height<<"\n");

		// Initialize the comparison objects.
		for (i = 0; i < dim; i++) {
			comp_obj_[i] = new typename POINT::cmp(i);
		}

		// Set the right block factor parameters for the case of an existing tree.
		params_.leaf_block_factor = pcoll_leaves_->block_factor();
		params_.node_block_factor = pcoll_nodes_->block_factor();

		// hack. (TODO: fix!)
		first_leaf_id_ = 1;

		bin_node_count_ = 0;

		TPLOG("kdtree::shared_init Exiting "<<"\n");
	}

//// *kdtree::create_bin_node* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::create_bin_node(TPIE_AMI_KDTREE_NODE *b, bn_context ctx, 
										  POINT_STREAM** in_streams, 
										  size_t& next_free_el, size_t& next_free_lk) {
		TPLOG("kdtree::create_bin_node Entering bid="<<b->bid()<<", dim="<<ctx.d<<"\n");

		MEMDISPLAY;
		HEIGHTDISPLAY_IN(" <>")

			POINT_STREAM* lo_streams[dim];
		POINT_STREAM* hi_streams[dim];

 
		POINT *p1, ap;
		TPIE_OS_OFFSET len = in_streams[ctx.d]->stream_len();
		assert(len > (TPIE_OS_OFFSET)params_.leaf_size_max);
		TPLOG("  kdtree::create_bin_node in_len="<<len<<"\n");
  
		// Go to the median position in the in_stream sorted on d'th coordinate.
		in_streams[ctx.d]->seek(median(len));
  
		// Read the median point. No need to read further. All points with
		// the same value on the d'th coordinate will go in the low stream
		// when distributing.
		err err = in_streams[ctx.d]->read_item(&p1);
		assert(err == tpie::ami::NO_ERROR);
		// Save it in ap.
		ap = *p1;
  
		// b->el[ctx.i] is the binary node we're constructing.
		b->el[ctx.i].initialize(p1->key, ctx.d);
		TPLOG("  kdtree::create_bin_node discriminator="<<(*p1)[ctx.d]<<", dim="<<ctx.d<<"\n");
  
		// Create the output streams by distributing the input streams.
		for (size_t i = 0; i < dim; i++) {

			// Make sure we start from 0 in stream i.
			in_streams[i]->seek(0);
			// Create the new streams.
			lo_streams[i] = new POINT_STREAM;
			lo_streams[i]->persist(PERSIST_DELETE);
			hi_streams[i] = new POINT_STREAM;
			hi_streams[i]->persist(PERSIST_DELETE);

			// Distribute.
			while ((err = in_streams[i]->read_item(&p1)) == tpie::ami::NO_ERROR) {
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
				((comp_obj_[ctx.d]->compare(*p1, ap) <= 0) ? lo_streams[i] : hi_streams[i])->write_item(*p1);
#else
				((b->el[ctx.i].discriminate(p1->key) <= 0) ? lo_streams[i] : hi_streams[i])->write_item(*p1);
#endif
			}

			assert(err == tpie::ami::END_OF_STREAM);
    
			assert(lo_streams[i]->stream_len() < in_streams[i]->stream_len());
			TPLOG("  kdtree::create_bin_node lo_len="<<lo_streams[i]->stream_len()<<"\n");
			assert(hi_streams[i]->stream_len() < in_streams[i]->stream_len());
			TPLOG("  kdtree::create_bin_node hi_len="<<hi_streams[i]->stream_len()<<"\n");
    
			// Remove the input stream.
			delete in_streams[i];
			in_streams[i] = NULL;
		}
  
		///DBG("create_bin_node: p=(" << ap[0] << "," << ap[1] << ") d=" << ctx.d << " pc_lo=" << lo_streams[0]->stream_len() << " pc_hi=" << hi_streams[0]->stream_len() << "\n");

		///assert(lo_streams[0]->stream_len() % 2 == 0);

		// The recursive calls...

		// ...for the low child...
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
		b->el[ctx.i].low_weight() = lo_streams[0]->stream_len();
#endif
		if (lo_streams[0]->stream_len() <= (TPIE_OS_OFFSET)params_.leaf_size_max) {

			b->el[ctx.i].set_low_child(next_free_lk++, BLOCK_LEAF);
			TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].lo: ("<<next_free_lk-1<<", BLOCK_LEAF)\n");
			create_leaf(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, lo_streams);

		} else if (can_do_mm(lo_streams[0]->stream_len())) {

			POINT* lo_streams_mm[dim];
			size_t lo_sz;
			copy_to_mm(lo_streams, lo_streams_mm, lo_sz);

			if ((ctx.h + 1 >= max_intranode_height(b->bid())) || 
				(next_free_el >= params_.node_size_max)) {

				b->el[ctx.i].set_low_child(next_free_lk++, BLOCK_NODE);
				TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].lo: ("<<next_free_lk-1<<", BLOCK_NODE)\n");
				create_node_mm(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, lo_streams_mm, lo_sz);

			} else {

				b->el[ctx.i].set_low_child(next_free_el++, BIN_NODE);
				TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].lo: ("<<next_free_el-1<<", BIN_NODE)\n");
				create_bin_node_mm(b, bn_context(next_free_el - 1, ctx.h + 1, (ctx.d + 1) % dim),
								   lo_streams_mm, lo_sz, next_free_el, next_free_lk);
			}

		} else { // Cannot load in memory. Continue normal algorithm.

			if ((ctx.h + 1 >= max_intranode_height(b->bid())) || 
				(next_free_el >= params_.node_size_max)) {

				b->el[ctx.i].set_low_child(next_free_lk++, BLOCK_NODE);
				create_node(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, lo_streams);

			} else {

				b->el[ctx.i].set_low_child(next_free_el++, BIN_NODE);
				create_bin_node(b, bn_context(next_free_el - 1, ctx.h + 1, (ctx.d + 1) % dim),
								lo_streams, next_free_el, next_free_lk);
			}
		}


		// ...and for the high child.
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
		b->el[ctx.i].high_weight() = hi_streams[0]->stream_len();
#endif
		if (hi_streams[0]->stream_len() <= (TPIE_OS_OFFSET)params_.leaf_size_max) {

			b->el[ctx.i].set_high_child(next_free_lk++, BLOCK_LEAF);
			TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].hi: ("<<next_free_lk-1<<", BLOCK_LEAF)\n");
			create_leaf(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, hi_streams);

		} else if (can_do_mm(hi_streams[0]->stream_len())) {

			POINT* hi_streams_mm[dim];
			size_t hi_sz;
			copy_to_mm(hi_streams, hi_streams_mm, hi_sz);

			if ((ctx.h + 1 >= max_intranode_height(b->bid())) || 
				(next_free_el >= params_.node_size_max)) {
				b->el[ctx.i].set_high_child(next_free_lk++, BLOCK_NODE);
				TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].hi: ("<<next_free_lk-1<<", BLOCK_NODE)\n");
				create_node_mm(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, hi_streams_mm, hi_sz);

			} else {

				b->el[ctx.i].set_high_child(next_free_el++, BIN_NODE);
				TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].hi: ("<<next_free_el-1<<", BIN_NODE)\n");
				create_bin_node_mm(b, bn_context(next_free_el - 1, ctx.h + 1, (ctx.d + 1) % dim),
								   hi_streams_mm, hi_sz, next_free_el, next_free_lk);
			}

		} else { // Cannot load in memory. Continue normal algorithm.

			if ((ctx.h + 1 >= max_intranode_height(b->bid())) || 
				(next_free_el >= params_.node_size_max)) {
      
				b->el[ctx.i].set_high_child(next_free_lk++, BLOCK_NODE);
				create_node(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, hi_streams);
      
			} else {
      
				b->el[ctx.i].set_high_child(next_free_el++, BIN_NODE);
				create_bin_node(b, bn_context(next_free_el - 1, ctx.h + 1, (ctx.d + 1) % dim),
								hi_streams, next_free_el, next_free_lk);
			}
		}
  
		HEIGHTDISPLAY_OUT
			TPLOG("kdtree::create_bin_node Exiting bid="<<b->bid()<<"\n");
	}


//// *kdtree::create_node* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::create_node(bid_t& bid, TPIE_OS_SIZE_T d, 
									  POINT_STREAM** in_streams) {
		TPLOG("kdtree::create_node Entering dim="<<d<<"\n");

		// New node.
		TPIE_AMI_KDTREE_NODE* n = fetch_node(); 
		bid = n->bid();
		n->weight() = in_streams[0]->stream_len();

		assert(d < dim);
		assert(in_streams[0]->stream_len() > (TPIE_OS_OFFSET)params_.leaf_size_max);

		bn_context ctx(0, 0, d);
		size_t next_free_el = 1; // because the root bin node goes in pos 0.
		size_t next_free_lk = 0;
  
		if (can_do_mm(in_streams[0]->stream_len())) {

			POINT* in_streams_mm[dim];
			size_t sz;
			copy_to_mm(in_streams, in_streams_mm, sz);
			create_bin_node_mm(n, ctx, in_streams_mm, sz, next_free_el, next_free_lk);

		} else
			create_bin_node(n, ctx, in_streams, next_free_el, next_free_lk);

		n->size() = next_free_el;
		bin_node_count_ += n->size();
		release_node(n);

		TPLOG("kdtree::create_node Exiting bid="<<bid<<"\n");
	}


//// *kdtree::create_leaf* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::create_leaf(bid_t& bid, TPIE_OS_SIZE_T d, 
									  POINT_STREAM** in_streams) {
		TPLOG("kdtree::create_leaf Entering "<<"\n");
		HEIGHTDISPLAY_IN(" O ")

			// New leaf.
			TPIE_AMI_KDTREE_LEAF* l = fetch_leaf();
		bid = l->bid();
		assert(d < dim);

		in_streams[d]->seek(0);
		assert(in_streams[d]->stream_len() <= (TPIE_OS_OFFSET)params_.leaf_size_max);
  
		// We are constructing a leaf, so we know that we have
		// little enough points to safely cast.
		l->size() = (TPIE_OS_SIZE_T)in_streams[d]->stream_len();

		if (previous_leaf_ == NULL) {
			first_leaf_id_ = l->bid();
		} else {
			previous_leaf_->next() = l->bid();
			release_leaf(previous_leaf_);
		}
		previous_leaf_ = l;

		// Copy points from stream to leaf. This should be an array copy,
		// but we don't have the mechanism...
		POINT *p;
		size_t i;
		for (i = 0; i < l->size(); i++) {
			in_streams[d]->read_item(&p);
			l->el[i] = *p;
		}

		// Remove the input streams.
		for (i = 0; i < dim; i++) {
			delete in_streams[i];
			in_streams[i] = NULL;
		}

		HEIGHTDISPLAY_OUT
			TPLOG("kdtree::create_leaf Exiting bid="<<bid<<", dim="<<d<<"\n");
	}

//// *kdtree::create_bin_node_mm* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::create_bin_node_mm(TPIE_AMI_KDTREE_NODE *b, bn_context ctx, 
											 POINT** in_streams, TPIE_OS_SIZE_T sz,
											 size_t& next_free_el, size_t& next_free_lk) {
		TPLOG("kdtree::create_bin_node_mm Entering bid="<<b->bid()<<", dim="<<ctx.d<<", idx="<<ctx.i<<"\n");
		MEMDISPLAY;
		HEIGHTDISPLAY_IN("-M-");

		POINT* lo_streams[dim];
		POINT* hi_streams[dim];
		POINT* current_stream;
		TPIE_OS_SIZE_T lo_sz;

		assert(sz > params_.leaf_size_max);
		POINT *p1, *p2, ap;
		TPIE_OS_SIZE_T read_pos;
		TPIE_OS_SIZE_T hi_j, lo_j, i, j;

		if (points_are_sample)
			// Get the real median. There's no point in doing anything fancy
			// when building on top of a sample.
			read_pos = real_median(sz); 
		else
			read_pos = median(sz);

		current_stream = in_streams[ctx.d];

		// Read a point.
		p1 = &current_stream[read_pos++];
		assert(read_pos < sz);

		// Read another point.
		p2 = &current_stream[read_pos];
  
		// Verify sorted order.
		assert((*p1)[ctx.d] <= (*p2)[ctx.d]);
  
		ap = *p1;
  
		// Initialize the binary node we are constructing.
		b->el[ctx.i].initialize(p1->key, ctx.d);  

		// Keep reading until we can discriminate between the two points,
		// in order to get the exact position. We need the exact position
		// to be able to allocate memory.
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
		while (read_pos < sz && comp_obj_[ctx.d]->compare(*p2, ap) == 0)
			p2 = &current_stream[++read_pos];
#else
		while (read_pos < sz && b->el[ctx.i].discriminate(p2->key) == 0)
			p2 = &current_stream[++read_pos];
#endif
    
		if (read_pos == sz) {
			// Take drastic measures.
			read_pos = real_median(sz);
			p1 = &current_stream[read_pos++];
			p2 = &current_stream[read_pos];
			ap = *p1;
			b->el[ctx.i].initialize(p1->key, ctx.d);
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
			while (read_pos < sz && comp_obj_[ctx.d]->compare(*p2, ap) == 0)
				p2 = &current_stream[++read_pos];
#else
			while (read_pos < sz && b->el[ctx.i].discriminate(p2->key) == 0)
				p2 = &current_stream[++read_pos];
#endif
		}

		// Hopefully, we didn't hit the end. (TODO: what happens if we do?)
		assert(read_pos < sz);
		assert(read_pos >= 1);

		lo_sz = read_pos;

		// Create the output streams by distributing the input streams.
		for (i = 0; i < dim; i++) {

			lo_streams[i] = new POINT[lo_sz];
			hi_streams[i] = new POINT[sz - lo_sz];
			// Distribute.
			lo_j = hi_j = 0;
			for (j = 0; j < sz; j++) {
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
				if (comp_obj_[ctx.d]->compare(in_streams[i][j], ap) <= 0)
					lo_streams[i][lo_j++] = in_streams[i][j];
#else
				if (b->el[ctx.i].discriminate(in_streams[i][j].key) <= 0)
					lo_streams[i][lo_j++] = in_streams[i][j];
#endif
				else
					hi_streams[i][hi_j++] = in_streams[i][j];
			}
			assert(lo_j == lo_sz);
			assert(hi_j == sz - lo_sz);
			delete [] in_streams[i];
			in_streams[i] = NULL;
		}

		// The recursive calls...

		// ...for the low child...
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
		b->el[ctx.i].low_weight() = lo_sz;
#endif
		if (lo_sz <= params_.leaf_size_max) {

			if (points_are_sample) {

				b->el[ctx.i].set_low_child(gso->q.size(), GRID_INDEX);
				gso->q.push_back(sample_context(b->bid(), ctx, true));
				for (TPIE_OS_SIZE_T  ii = 0; ii < dim; ii++) {
					delete [] lo_streams[ii];
					lo_streams[ii] = NULL;
				}

			} else {
				b->el[ctx.i].set_low_child(next_free_lk++, BLOCK_LEAF);
				TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].lo: ("<<next_free_lk-1<<", BLOCK_LEAF)\n");
				create_leaf_mm(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, 
							   lo_streams, lo_sz);
			}
		} else if ((ctx.h + 1 >= max_intranode_height(b->bid())) || 
				   (next_free_el >= params_.node_size_max)) {

			b->el[ctx.i].set_low_child(next_free_lk++, BLOCK_NODE);
			TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].lo: ("<<next_free_lk-1<<", BLOCK_NODE)\n");
			create_node_mm(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, 
						   lo_streams, lo_sz);

		} else {

			b->el[ctx.i].set_low_child(next_free_el++, BIN_NODE);
			TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].lo: ("<<next_free_el-1<<", BIN_NODE)\n");
			create_bin_node_mm(b, bn_context(next_free_el - 1, ctx.h + 1, 
											 (ctx.d + 1) % dim),
							   lo_streams, lo_sz, next_free_el, next_free_lk);
		}

		// We must have made at least one external link.
		//  assert(next_free_lk > 0);
		TPLOG("  kdtree::create_bin_node_mm Mid-recursion bid="<<b->bid()<<"\n");

		// ...and for the high child.
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
		b->el[ctx.i].high_weight() = sz - lo_sz;
#endif
		if (sz - lo_sz <= params_.leaf_size_max) {

			if (points_are_sample) {
				b->el[ctx.i].set_high_child(gso->q.size(), GRID_INDEX);
				gso->q.push_back(sample_context(b->bid(), ctx, false));
				for (TPIE_OS_SIZE_T ii = 0; ii < dim; ii++) {
					delete [] hi_streams[ii];
					hi_streams[ii] = NULL;
				}
			} else {
				b->el[ctx.i].set_high_child(next_free_lk++, BLOCK_LEAF);
				TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].hi: ("<<next_free_lk-1<<", BLOCK_LEAF)\n");
				create_leaf_mm(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, 
							   hi_streams, sz - lo_sz);
			}
		} else if ((ctx.h + 1 >= max_intranode_height(b->bid())) || 
				   (next_free_el >= params_.node_size_max)) {

			b->el[ctx.i].set_high_child(next_free_lk++, BLOCK_NODE);
			TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].hi: ("<<next_free_lk-1<<", BLOCK_NODE)\n");
			create_node_mm(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, 
						   hi_streams, sz - lo_sz);

		} else {

			b->el[ctx.i].set_high_child(next_free_el++, BIN_NODE);
			TPLOG("  b("<<b->bid()<<")->el["<<ctx.i<<"].hi: ("<<next_free_el-1<<", BIN_NODE)\n");
			create_bin_node_mm(b, bn_context(next_free_el - 1, ctx.h + 1, (ctx.d + 1) % dim),
							   hi_streams, sz - lo_sz, next_free_el, next_free_lk);
		}

		//  assert(next_free_lk > 1);
		HEIGHTDISPLAY_OUT;
		TPLOG("kdtree::create_bin_node_mm Exiting bid="<<b->bid()<<"\n");
	}

//// *kdtree::create_leaf_mm* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::create_leaf_mm(bid_t& bid, TPIE_OS_SIZE_T, 
										 POINT** in_streams, TPIE_OS_SIZE_T sz) {
		TPLOG("kdtree::create_leaf_mm Entering "<<"\n");

		// Brand new leaf.
		TPIE_AMI_KDTREE_LEAF* l = fetch_leaf();
		bid = l->bid();
		assert(sz <= params_.leaf_size_max);

		l->size() = sz;

		if (previous_leaf_ == NULL) {
			first_leaf_id_ = l->bid();
		} else {
			previous_leaf_->next() = l->bid();
			release_leaf(previous_leaf_);
		}
		previous_leaf_ = l;

		size_t i;

		// Copy the points. 
		l->el.copy(0, sz, in_streams[0]);

		// Remove the input streams.
		for (i = 0; i < dim; i++) {
			delete [] in_streams[i];
			in_streams[i] = NULL;
		}

		TPLOG("kdtree::create_leaf_mm Exiting bid="<<bid<<", dim="<<d<<"\n");  
	}


//// *kdtree::create_node_mm* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::create_node_mm(bid_t& bid, TPIE_OS_SIZE_T d, 
										 POINT** in_streams, TPIE_OS_SIZE_T sz) {
		TPLOG("kdtree::create_node_mm Entering dim="<<d<<"\n");

		// New node.
		TPIE_AMI_KDTREE_NODE* n = fetch_node();
		bid = n->bid();
		n->weight() = sz;
  
		assert(d < dim);
		assert(sz > params_.leaf_size_max);

		bn_context ctx(0, 0, d);
		size_t next_free_el = 1; // because the root bin node goes in pos 0.
		size_t next_free_lk = 0;
		n->lk[n->lk.capacity()-1] = 0;

		create_bin_node_mm(n, ctx, in_streams, sz, next_free_el, next_free_lk);

		n->size() = next_free_el;
		//..
		if (n->lk[n->lk.capacity()-1] == 0)
			n->lk[n->lk.capacity()-1] = next_free_lk;
		bin_node_count_ += n->size();
		release_node(n);
		TPLOG("kdtree::create_node_mm Exiting bid="<<bid<<"\n");
	}

//// *kdtree::can_do_mm* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	bool TPIE_AMI_KDTREE::can_do_mm(TPIE_OS_OFFSET sz) {
		bool ans = ((TPIE_OS_OFFSET) (sz * sizeof(POINT) * TPIE_OS_OFFSET(dim + 1) + 
					pcoll_nodes_->block_size() * params_.node_cache_size +
					pcoll_leaves_->block_size() * params_.leaf_cache_size +
									  TPIE_OS_OFFSET(8192 * 4)) < (TPIE_OS_OFFSET) get_memory_manager().available());
		TPLOG("kdtree::can_do_mm needed = " << 
			  (TPIE_OS_OFFSET) ((TPIE_OS_OFFSET) sz * sizeof(POINT) * TPIE_OS_OFFSET(dim + 1) + 
								pcoll_nodes_->block_size() * params_.node_cache_size +
								pcoll_leaves_->block_size() * params_.leaf_cache_size +
								TPIE_OS_OFFSET(8192 * 4)) << ", avail = " << 
			  get_memory_manager().available() << " ans = " << ans << "\n");
		return ans;
	}

//// *kdtree::copy_to_mm* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::copy_to_mm(POINT_STREAM** in_streams, POINT** mm_streams, TPIE_OS_SIZE_T& sz) {
		TPLOG("kdtree::copy_to_mm Entering "<<"\n");
		// The call to this method should have been preceeded by a call
		// to can_do_mm, so casting should be o.k.
		sz = (TPIE_OS_SIZE_T)in_streams[0]->stream_len();
		TPIE_OS_SIZE_T i, j;
		POINT* p;
   
		for (i = 0; i < dim; i++) {
			mm_streams[i] = new POINT[sz];
			in_streams[i]->seek(0);
			j = 0;
			while (in_streams[i]->read_item(&p) == tpie::ami::NO_ERROR) {
				mm_streams[i][j++] = *p;
			}
			// Delete the stream.
			delete in_streams[i];
			in_streams[i] = NULL;
		}

		TPLOG("kdtree::copy_to_mm Exiting "<<"\n");
	}

//// *kdtree::copy_to_mm* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::copy_to_mm(POINT_STREAM* in_stream, POINT** streams_mm, TPIE_OS_SIZE_T& sz) {
		TPLOG("kdtree::copy_to_mm Entering "<<"\n");
		// This method call should have been preceeded by a call to can_do_mm
		// so casting should be o.k.
		sz = (TPIE_OS_SIZE_T)in_stream->stream_len();
		TPIE_OS_SIZE_T i, j;
		POINT* p;
		//  bool set_mbr;

		// Read from disk into the first in-memory stream.
		streams_mm[0] = new POINT[sz];
		in_stream->seek(0);
		i = 0;
		while (in_stream->read_item(&p) == tpie::ami::NO_ERROR)
			streams_mm[0][i++] = *p;
		//  delete in_stream;

		// Make dim-1 more in-memory copies.
		for (j = 1; j < dim; j++) {
			streams_mm[j] = new POINT[sz];
			//    memcpy(streams_mm[j], streams_mm[0], sz * sizeof(POINT));
			for (i = 0; i < sz; i++)
				streams_mm[j][i] = streams_mm[0][i];
		}

		// Sort the dim in-memory streams (and update the mbr).
		for (j = 0; j < dim; j++) {

			std::sort(streams_mm[j], streams_mm[j]+sz, *comp_obj_[j]);

			if (header_.mbr_lo.id() == 0 || header_.mbr_hi.id() == 0) {
				header_.mbr_lo[j] = streams_mm[j][0][j];
				header_.mbr_hi[j] = streams_mm[j][sz-1][j];
			} else {
				header_.mbr_lo[j] = std::min(streams_mm[j][0][j], header_.mbr_lo[j]);
				header_.mbr_hi[j] = std::max(streams_mm[j][sz-1][j], header_.mbr_hi[j]);
			}
		}

		if (header_.mbr_lo.id() == 0 || header_.mbr_hi.id() == 0) {
			header_.mbr_lo.id() = 1;
			header_.mbr_hi.id() = 1;
		}  

		TPLOG("kdtree::copy_to_mm Exiting "<<"\n");
	}

//// *kdtree::create_grid* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::create_grid(bid_t&, TPIE_OS_SIZE_T, POINT_STREAM** in_streams, TPIE_OS_SIZE_T t) {
		TPLOG("kdtree::create_grid Entering "<<"\n");
		// Note: only one grid level implemented.

		DBG("  Computing grid lines [" << (TPIE_OS_OFFSET)dim*t << " (random seek + read)]...\n");
		grid *g = new grid(t, in_streams);

		DBG("  Creating matrix [1 linear scan]...\n");
		grid_matrix* gmx = g->create_matrix();

		// Create log(t) levels.
		DBG("  Creating top levels [very few node writes]...\n");
		create_node_g(header_.root_bid, 0, gmx);

		// Distribute the points.
		DBG("  Distributing in " << (TPIE_OS_OFFSET)g->q.size() << "x" << (TPIE_OS_OFFSET)dim << " streams [" << (TPIE_OS_OFFSET)dim << " linear scans, distribution writing]...\n");
		distribute_g(header_.root_bid, 0, g);

		DBG("  Building lower levels [" << (TPIE_OS_OFFSET)g->q.size() << "x" << (TPIE_OS_OFFSET)dim << " small linear scans, lots of block writes]...\n");
		build_lower_tree_g(g);

		delete g;
		TPLOG("kdtree::create_grid Exiting "<<"\n");
	}

//// *kdtree::build_lower_tree_g* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::build_lower_tree_g(grid* g) {
		TPLOG("kdtree::build_lower_tree_g Entering "<<"\n");

		grid_context *gc;
		size_t sz, i, j;
		//  POINT* p;
		POINT* streams_mm[dim];
		//  err err;
		TPIE_OS_SIZE_T next_free_el;
		TPIE_OS_SIZE_T next_free_lk;
		TPIE_AMI_KDTREE_NODE* b;

		for (j = 0; j < g->q.size(); j++) {

			gc = &(g->q[j]);
			b = fetch_node(gc->bid);
			next_free_el = b->size();
			next_free_lk = (TPIE_OS_SIZE_T)b->lk[b->lk.capacity()-1];
			b->lk[b->lk.capacity()-1] = 0;

			// Create the streams and load them into memory.
			DBG("L");
			for (i = 0; i < dim; i++) {
				gc->streams[i] = new POINT_STREAM(gc->stream_names[i]);
				gc->streams[i]->persist(PERSIST_DELETE);
				if (gc->streams[i]->status() == tpie::ami::STREAM_STATUS_INVALID) {
					std::cerr << "kdtree bulk loading internal error.\n" 
							  << "[invalid stream restored from file "
							  << gc->stream_names[i] << "].\n";
					std::cerr << "Aborting.\n";
					delete gc->streams[i];
					exit(1);
				}
			}

			copy_to_mm(gc->streams, streams_mm, sz);

			// Build the subtree.
			DBG("B"<<(TPIE_OS_OFFSET)sz);
			if (sz <= params_.leaf_size_max) {

				if (gc->low)
					b->el[gc->ctx.i].set_low_child(next_free_lk++, BLOCK_LEAF);
				else
					b->el[gc->ctx.i].set_high_child(next_free_lk++, BLOCK_LEAF);
				TPLOG("  b("<<b->bid()<<")->el["<<gc->ctx.i<<"].?: ("<<next_free_lk-1<<", BLOCK_LEAF)\n");
				create_leaf_mm(b->lk[next_free_lk - 1], (gc->ctx.d + 1) % dim, streams_mm, sz);

			} else if ((gc->ctx.h + 1 >= max_intranode_height(b->bid())) || 
					   (next_free_el >= params_.node_size_max)) {

				if (gc->low)
					b->el[gc->ctx.i].set_low_child(next_free_lk++, BLOCK_NODE);
				else
					b->el[gc->ctx.i].set_high_child(next_free_lk++, BLOCK_NODE);
				TPLOG("  b("<<b->bid()<<")->el["<<gc->ctx.i<<"].?: ("<<next_free_lk-1<<", BLOCK_NODE)\n");
				create_node_mm(b->lk[next_free_lk - 1], (gc->ctx.d + 1) % dim, streams_mm, sz);

			} else {

				if (gc->low)
					b->el[gc->ctx.i].set_low_child(next_free_el++, BIN_NODE);
				else
					b->el[gc->ctx.i].set_high_child(next_free_el++, BIN_NODE);
				TPLOG("  b("<<b->bid()<<")->el["<<gc->ctx.i<<"].?: ("<<next_free_el-1<<", BIN_NODE)\n");
				///DBG("create_grid: sz=" << sz << "\n"); 
				create_bin_node_mm(b, bn_context(next_free_el - 1, gc->ctx.h + 1, 
												 (gc->ctx.d + 1) % dim),
								   streams_mm, sz, next_free_el, next_free_lk);
			}

			// save the next_free_* info.
			if (b->lk[b->lk.capacity()-1] == 0) {
				b->size() = next_free_el;
				b->lk[b->lk.capacity()-1] = next_free_lk;
			}
    
			release_node(b);
			DBG(" ");
		}
		TPLOG("kdtree::build_lower_tree_g Exiting "<<"\n");
	}

//// *kdtree::distribute_g* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::distribute_g(bid_t, TPIE_OS_SIZE_T, grid* g) {
		TPLOG("TPIE_AMI_KDTREE::distribute_g Entering\n");
		err err;
		TPIE_OS_SIZE_T i, j, jj;
		TPIE_OS_OFFSET sz;
		POINT* p1;

		TPLOG("  Queue size: " << g->q.size());
		// Create all streams. Could we run out of memory here?
		for (j = 0; j < g->q.size(); j++) {
			for (i = 0; i < dim; i++) {
				g->q[j].streams[i] = new POINT_STREAM;
				g->q[j].stream_names[i] = g->q[j].streams[i]->name();
				g->q[j].streams[i]->persist(PERSIST_PERSISTENT);
			}
		}

#if NEW_DISTRIBUTE_G
		jj = 0;
		sz = dim * g->streams[0]->stream_len();
		for (i = 0; i < dim; i++) {
			g->streams[i]->seek(0);
			while ((err = g->streams[i]->read_item(&p1)) == tpie::ami::NO_ERROR) {
				if (jj % 200000 == 0)
					DBG("\b\b\b"<<TPIE_OS_OFFSET(((float)jj / sz) * 100.0)<<"%");
				jj++;

				for (j = 0; j < g->q.size(); j++) {
					if (g->q[j].gmx.is_inside(*p1)) {
						break;
					}
				}
				// All points must be distributed, since no leaves were created so far.
				assert(j < g->q.size());
      
				g->q[j].streams[i]->write_item(*p1);
			}
		}
		DBG("\b\b\b   \b\b\b");

#else
		// The root of this sub-tree is *r.
		TPIE_AMI_KDTREE_NODE *n, *r = fetch_node(bid);
		bid_t nbid;
		grid_context* gc;
		std::pair<size_t, link_type_t> a;

		// Distribute points.
		for (i = 0; i < dim; i++) {
			g->streams[i]->seek(0);
			while ((err = g->streams[i]->read_item(&p1)) == tpie::ami::NO_ERROR) {
				// Find the index in the q vector.
				a = r->find_index(*p1);
				if (a.second == BLOCK_NODE)
					nbid = r->lk[a.first];
				while (a.second == BLOCK_NODE) {
					n = fetch_node(nbid);
					a = n->find_index(*p1);
					if (a.second == BLOCK_NODE)
						nbid = n->lk[a.first];
					release_node(n);
				}
				// Make sure there are no leaves or other funny stuff.
				assert(a.second == GRID_INDEX);

				gc = &g->q[a.first];
				gc->streams[i]->write_item(*p1);
			}
		}

		release_node(r);
#endif

		// Delete input streams.
		for (i = 0; i < dim; i++) {
			delete g->streams[i];
			g->streams[i] = NULL;
		}
		// Delete output streams. We'll read them later, one by one.
		for (j = 0; j < g->q.size(); j++) {
			for (i = 0; i < dim; i++) {
				delete g->q[j].streams[i];
				g->q[j].streams[i] = NULL;
			}
		}

		TPLOG("TPIE_AMI_KDTREE::distribute_g Exiting\n");
	}

//// *kdtree::create_node_g* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::create_node_g(bid_t& bid, TPIE_OS_SIZE_T d, grid_matrix* gmx) {
		TPLOG("kdtree::create_node_g Entering "<<"\n");
  
		TPIE_AMI_KDTREE_NODE *n = fetch_node();
		bid = n->bid();
		n->weight() = gmx->point_count;

		assert(d < dim);

		bn_context ctx(0, 0, d);
		size_t next_free_el = 1; // because the root bin node goes in pos 0.
		size_t next_free_lk = 0;
		n->lk[n->lk.capacity()-1] = 0;
  
		create_bin_node_g(n, ctx, gmx, next_free_el, next_free_lk);

		n->size() = next_free_el;
		// Store next_free_el and next_free_lk in n.
		if (n->lk[n->lk.capacity()-1] == 0) {
			n->lk[n->lk.capacity()-1] = next_free_lk;
		}
		bin_node_count_ += n->size();
		release_node(n);

		TPLOG("kdtree::create_node_g Exiting bid="<<bid<<"\n");
	} 

//// *kdtree::create_bin_node_g* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	void TPIE_AMI_KDTREE::create_bin_node_g(TPIE_AMI_KDTREE_NODE *b, bn_context ctx, grid_matrix *gmx, 
											size_t& next_free_el, size_t& next_free_lk) {
		TPLOG("kdtree::create_bin_node_g Entering "<<"\n");
   
		grid_matrix* gmx_hi; // gmx will act as gmx_lo.

		POINT p;
		// Find the median and split the current matrix.
		gmx_hi = gmx->find_median_and_split(p, ctx.d, median(gmx->point_count));
		// Initialize the binary node.
		b->el[ctx.i].initialize(p.key, ctx.d);

		///DBG("create_bin_node_g: p=(" << p[0] << "," << p[1] << ") d=" << ctx.d << " pc_lo=" << gmx->point_count << " pc_hi=" << gmx_hi->point_count << "\n");

		///assert(gmx->point_count % 2 == 0);
#define USE_GRID_MORE 0

#if USE_GRID_MORE
		if (gmx->point_count > params_.leaf_size_max && 
			(ctx.h + 1 < max_intranode_height(b->bid())) && 
			next_free_el < params_.node_size_max) {
			b->el[ctx.i].set_low_child(next_free_el++, BIN_NODE);
			create_bin_node_g(b, bn_context(next_free_el - 1, ctx.h + 1, (ctx.d + 1) % dim), 
							  gmx, next_free_el, next_free_lk);  
		} else
#endif
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
			b->el[ctx.i].low_weight() = gmx->point_count;
#endif
		if (can_do_mm(gmx->point_count)) {
			// Put a marker in the tree and exit. The actual buiding will be done later.

			b->el[ctx.i].set_low_child(gmx->g->q.size(), GRID_INDEX);
			// Push the current context into the grid queue.
			gmx->g->q.push_back(grid_context(b->bid(), ctx, true, *gmx));
			delete gmx;

		} else {

			if ((ctx.h + 1 >= max_intranode_height(b->bid())) || 
				(next_free_el >= params_.node_size_max)) {
				b->el[ctx.i].set_low_child(next_free_lk++, BLOCK_NODE);
				create_node_g(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, gmx);
			} else {       
				b->el[ctx.i].set_low_child(next_free_el++, BIN_NODE);
				create_bin_node_g(b, bn_context(next_free_el - 1, ctx.h + 1, (ctx.d + 1) % dim), 
								  gmx, next_free_el, next_free_lk);
			}
		}
     

#if USE_GRID_MORE
		if (gmx_hi->point_count > params_.leaf_size_max && 
			(ctx.h + 1 < max_intranode_height(b->bid())) && 
			next_free_el < params_.node_size_max) {
			b->el[ctx.i].set_high_child(next_free_el++, BIN_NODE);
			create_bin_node_g(b, bn_context(next_free_el - 1, ctx.h + 1, (ctx.d + 1) % dim), 
							  gmx_hi, next_free_el, next_free_lk);  
		} else
#endif
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
			b->el[ctx.i].high_weight() = gmx_hi->point_count;
#endif
		if (can_do_mm(gmx_hi->point_count)) {

			b->el[ctx.i].set_high_child(gmx_hi->g->q.size(), GRID_INDEX);
			// Push the current context into the grid queue.
			gmx_hi->g->q.push_back(grid_context(b->bid(), ctx, false, *gmx_hi));
			delete gmx_hi;

		} else {

			if ((ctx.h + 1 >= max_intranode_height(b->bid())) || 
				(next_free_el >= params_.node_size_max)) {
				b->el[ctx.i].set_high_child(next_free_lk++, BLOCK_NODE);
				create_node_g(b->lk[next_free_lk - 1], (ctx.d + 1) % dim, gmx_hi);
			} else {
				b->el[ctx.i].set_high_child(next_free_el++, BIN_NODE);
				create_bin_node_g(b, bn_context(next_free_el - 1, ctx.h + 1, (ctx.d + 1) % dim), 
								  gmx_hi, next_free_el, next_free_lk);
			}
		}

		TPLOG("kdtree::create_bin_node_g Exiting bid="<<b->bid()<<"\n");   
	}

//// *kdtree::sort* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	err TPIE_AMI_KDTREE::sort(POINT_STREAM* in_stream, POINT_STREAM* out_streams[]) {
		TPLOG("kdtree::sort Entering"<<"\n");

		if (in_stream == NULL) {
			TP_LOG_WARNING_ID("Attempting to sort a NULL stream pointer. Sorting Aborted.");
			return tpie::ami::OBJECT_INITIALIZATION;
		}

		assert(in_stream->stream_len() > 0);
		size_t i;
		err err;

		for (i = 0; i < dim; i++) {
			// If necessary, create temporary stream for points sorted on the
			// ith coordinate.
			if (out_streams[i] == NULL) {
				out_streams[i] = new POINT_STREAM;
				out_streams[i]->persist(PERSIST_DELETE);
			}

			// Sort points on the ith coordinate.
			err = tpie::ami::sort(in_stream, out_streams[i], comp_obj_[i]);
			if (err != tpie::ami::NO_ERROR)
				break;
			assert(in_stream->stream_len() == out_streams[i]->stream_len());

		}
		if (err != tpie::ami::NO_ERROR) {
			TP_LOG_WARNING_ID("Sorting returned error.");
		}

		TPLOG("kdtree::sort Exiting err="<<err<<"\n");
		return err;
	}

//// *kdtree::load_sorted* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	err TPIE_AMI_KDTREE::load_sorted(POINT_STREAM* streams_s[], 
									 float lfill, float nfill, int load_method) {
		TPLOG("kdtree::load_sorted Entering"<<"\n");
		err err = tpie::ami::NO_ERROR;

		// Some error checking.
		if (header_.size > 0) {
			TP_LOG_WARNING_ID("kdtree already loaded. Nothing done in load.");
			return tpie::ami::GENERIC_ERROR;
		}
		if (status_ == KDTREE_STATUS_INVALID) {
			TP_LOG_WARNING_ID("kdtree is invalid. Nothing done in load.");
			return tpie::ami::OBJECT_INITIALIZATION;
		}
		if (streams_s[0] == NULL) {
			TP_LOG_WARNING_ID("Attempting to load with a NULL stream pointer. Aborted.");
			return tpie::ami::OBJECT_INITIALIZATION;
		}

		header_.size = streams_s[0]->stream_len();
		first_leaf_id_ = 0;
		previous_leaf_ = NULL;

		// Set max_intraroot_height. 
		//if (params_.max_intranode_height == params_.max_intraroot_height) 
		//  params_.max_intraroot_height = 
		//   min((size_t) (log((double)header_.size/params_.leaf_size_max)/log(2)) 
		//   % params_.max_intranode_height + 1, params_.max_intranode_height);

		kdtree_params params_saved = params_;
		params_.leaf_size_max = std::min(params_.leaf_size_max, 
										 size_t(lfill*params_.leaf_size_max));
		params_.node_size_max = std::min(params_.node_size_max, 
										 size_t(nfill*params_.node_size_max));

		// Reinitialize params_.max_intranode_height
		if (params_.max_intranode_height == params_.max_intraroot_height) {
			// First reset intranode height.
			size_t i;
			for (i = 0; i < 64; i++)
				if (params_.node_size_max >> i == 1)
					break;
			assert(i < 64);
			params_.max_intranode_height = i + 1;

			// Now reset intraroot height.
			params_.max_intraroot_height = 
				std::min((size_t) (log((double)header_.size/params_.leaf_size_max)/log(2.0)) 
						 % params_.max_intranode_height + 1, params_.max_intranode_height);
		}

		// Set the mbr.
		POINT *pp;
		size_t i;
		for (i = 0; i < dim; i++) {
			streams_s[i]->seek(0);
			streams_s[i]->read_item(&pp);
			header_.mbr_lo[i] = (*pp)[i];
			///DBG("mbr_lo[" << i << "]=" << header_.mbr_lo[i] << "\n");
			streams_s[i]->seek(header_.size - 1);
			streams_s[i]->read_item(&pp);
			header_.mbr_hi[i] = (*pp)[i];
			streams_s[i]->seek(0);
			///DBG("mbr_hi[" << i << "]=" << header_.mbr_hi[i] << "\n");
		}
		header_.mbr_lo.id() = 1;
		header_.mbr_hi.id() = 1;


		DBG("building (" << header_.size << ")...\n");
		MEMDISPLAY_INIT;
		HEIGHTDISPLAY_INIT;

		// The actual loading.
		if (header_.size <= (TPIE_OS_OFFSET)params_.leaf_size_max) {
			header_.root_type = BLOCK_LEAF;
			create_leaf(header_.root_bid, 0, streams_s);
		} else {

			header_.root_type = BLOCK_NODE;
			if (can_do_mm(header_.size)) {
				POINT* streams_mm[dim];
				TPIE_OS_SIZE_T sz;
				copy_to_mm(streams_s, streams_mm, sz);
				create_node_mm(header_.root_bid, 0, streams_mm, sz);
			} else if (load_method & TPIE_AMI_KDTREE_LOAD_BINARY) {
				// Build the tree using binary distribution.
				create_node(header_.root_bid, 0, streams_s);
			} else if (load_method & TPIE_AMI_KDTREE_LOAD_GRID) {
				// Build the tree using the grid method.
				create_grid(header_.root_bid, 0, streams_s, params_.grid_size);
			} else {
				TP_LOG_WARNING_ID("No other loading method implemented.");
				TP_LOG_WARNING_ID("Loading aborted.");
				err = tpie::ami::GENERIC_ERROR;
			}
		}

		status_ = KDTREE_STATUS_VALID; 

		if (previous_leaf_ != NULL) {
			previous_leaf_->next() = 0;
			release_leaf(previous_leaf_);
		}

		// Flush the caches. Not really necessary, but it helps free some
		// memory.
		node_cache_->flush();
		leaf_cache_->flush();
  
		MEMDISPLAY_DONE;
		HEIGHTDISPLAY_DONE;

		// Restore params_.
		params_ = params_saved;

		TPLOG("kdtree::load_sorted Exiting err="<<err<<"\n");
		return err;
	}


//// *kdtree::load* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	err TPIE_AMI_KDTREE::load(POINT_STREAM* s, float lfill, float nfill, int load_method) {
		TPLOG("kdtree::load Entering "<<"\n");
		POINT_STREAM* streams_s[dim];
		size_t i;
		err err;

		for (i = 0; i < dim; i++) {
			streams_s[i] = NULL;
		}

		// Sort.
		err = sort(s, streams_s);
		// Load.
		if (err == tpie::ami::NO_ERROR)
			err = load_sorted(streams_s, lfill, nfill, load_method);

		TPLOG("kdtree::load Exiting err="<<err<<"\n");
		return err;
	}

//// *kdtree::load_sample* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	err TPIE_AMI_KDTREE::load_sample(POINT_STREAM* s) {
		TPLOG("kdtree::load_sample Entering"<<"\n");

		err err = tpie::ami::NO_ERROR;
		header_.size = s->stream_len();
		first_leaf_id_ = 0;
		previous_leaf_ = NULL;

		// Set max_intraroot_height. 
		if (params_.max_intranode_height <= params_.max_intraroot_height) 
			params_.max_intraroot_height = 
				((size_t) (log((double)header_.size/params_.leaf_size_max)/log(2.0))
				 % params_.max_intranode_height + 1) % params_.max_intranode_height + 1;
  

		if (header_.size <= (TPIE_OS_OFFSET)params_.leaf_size_max) {

			header_.root_type = BLOCK_LEAF;
			std::cerr << "Size too small. Not implemented.\n";
			err = tpie::ami::GENERIC_ERROR;
			//    create_leaf(header_.root_bid, 0, s);

		} else {

			header_.root_type = BLOCK_NODE;
			if (can_do_mm(header_.size)) {

				POINT* streams_mm[dim];
				size_t sz;
				copy_to_mm(s, streams_mm, sz);
				create_node_mm(header_.root_bid, 0, streams_mm, sz);

			} else {

				// Build the tree using binary distribution.
				create_sample(header_.root_bid, 0, s);
			}
		}

		status_ = KDTREE_STATUS_VALID; 

		if (previous_leaf_ != NULL) {
			previous_leaf_->next() = 0;
			release_leaf(previous_leaf_);
		}

		// Flush the caches. Not really necessary, but it helps free some
		// memory.
		node_cache_->flush();
		leaf_cache_->flush();

		MEMDISPLAY_DONE;
		HEIGHTDISPLAY_DONE

			TPLOG("kdtree::load_sample Exiting err="<<err<<"\n");
		return err;
	}

//// *kdtree::unload* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	err TPIE_AMI_KDTREE::unload(POINT_STREAM* s) {
		TPLOG("kdtree::unload Entering "<<"\n");
		bid_t lid = first_leaf_id_;
		TPIE_AMI_KDTREE_LEAF *l;
		size_t i;
		err err = tpie::ami::NO_ERROR;
		///DBG("  first_leaf_id_=" << first_leaf_id_ << "\n");

		if (s == NULL) {
			TP_LOG_WARNING_ID("  unload: null stream pointer. unload aborted.");
			return tpie::ami::OBJECT_INITIALIZATION;
		}
		if (status_ != KDTREE_STATUS_VALID) {
			TP_LOG_WARNING_ID("  unload: tree is invalid or not loaded. unload aborted.");
			return tpie::ami::OBJECT_INITIALIZATION;
		}
		assert(lid != 0);

		while (lid != 0) {
			l = fetch_leaf(lid);
			for (i = 0; i < l->size(); i++)
				s->write_item(l->el[i]);
			lid = l->next();
			release_leaf(l);
		}
		TPLOG("kdtree::unload Exiting err="<<err<<"\n");
		return err;
	}

//// *kdtree::k_nn_query* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	TPIE_OS_OFFSET TPIE_AMI_KDTREE::k_nn_query(const POINT &p, 
											   POINT_STREAM* stream, TPIE_OS_OFFSET k) {
		unused(p);
		unused(stream);
		unused(k);
		TPLOG("kdtree::k_nn_query Entering "<<"\n");
		TPIE_OS_OFFSET result = 0;

		// Do some error checking.
		if (status_ != KDTREE_STATUS_VALID) {
			TP_LOG_WARNING_ID("  k_nn_query: tree is invalid or not loaded. query aborted.");
			return result;
		}

		std::cerr << "k_nn_query: NOT IMPLEMENTED YET!\n";
		TP_LOG_WARNING_ID("  k_nn_query: NOT IMPLEMENTED YET!");
		//  priority_std::queue<nn_pq_elem> q;
		//  nn_pq_elem cur((coord_t) 0, header_.root_bid, header_.root_type);
		//  while (cur.type != BLOCK_LEAF) {
		//  }  

		TPLOG("kdtree::k_nn_query Exiting "<<"\n");
		return result;
	}

//// *kdtree::window_query* ////
	template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
	TPIE_OS_OFFSET TPIE_AMI_KDTREE::window_query(const POINT &p1, const POINT& p2, 
												 POINT_STREAM* stream) {
		TPLOG("kdtree::window_query Entering "<<"\n");
		POINT lop, hip;
		// The number of points found.
		TPIE_OS_OFFSET result = 0;
		TPIE_OS_SIZE_T i;

		// Do some error checking.
		if (status_ != KDTREE_STATUS_VALID) {
			TP_LOG_WARNING_ID("  window_query: tree is invalid or not loaded. query aborted.");
			return result;
		}

		// Determine the low and high bounds of the box.
		for (i = 0; i < dim; i++) {
			lop[i] = std::min(p1[i], p2[i]);
			hip[i] = std::max(p1[i], p2[i]);
			if (p1[i] == p2[i]) {TP_LOG_WARNING_ID("  window_query: points have one identical coordinate.");}
		}

		// A stack for the search (no recursive calls here :). 
		std::stack<outer_stack_elem> s;

		// Another stack for the search inside a block node. The elements
		// are indexes in the el vector of a block node.
		std::stack<inner_stack_elem> ss;

		podf allfalse;
		for (i = 0; i < dim; i++) {
			allfalse.first[i] = false; // ie, low boundary of current box, on dim. i, is outside the query window.
			allfalse.second[i] = false; // ie, high boundary on dim. i is outside the query window.
		}

		s.push(outer_stack_elem(allfalse,
								std::pair<bid_t, link_type_t>(header_.root_bid, header_.root_type)));

		std::pair<bid_t,link_type_t> top;
		podf topflags, tempflags;
		TPIE_OS_SIZE_T child;
		link_type_t childtype;
		TPIE_AMI_KDTREE_NODE *bn, *bn2;
		TPIE_AMI_KDTREE_LEAF *bl;

		while (!s.empty()) {
			// Copy the top of the stack.
			top = s.top().second;
			topflags = s.top().first;
			s.pop();

			if (top.second == BLOCK_LEAF) {

				bl = fetch_leaf(top.first);
				result += bl->window_query(lop, hip, stream);
				release_leaf(bl);

			} else { // BLOCK_NODE

				assert(top.second == BLOCK_NODE);
				bn = fetch_node(top.first);

				// Inner stack should be empty.
				assert(ss.empty());

				// The first Bin_node in a block node always has index 0.
				ss.push(inner_stack_elem(topflags, 0));

				// The inner loop. Visit all relevant Bin_node's inside *bn.
				while (!ss.empty()) {
					Bin_node &v = bn->el[ss.top().second];
					// Recycle topflags.
					topflags = ss.top().first;
					ss.pop();

					// Check whether we need to visit the low child of v.
					if (v.discriminate(lop.key) <= 0 || v.discriminate(hip.key) <= 0) {
						// Push the low child into the appropriate stack.
						v.get_low_child(child, childtype);
						// Make a copy of topflags.
						tempflags = topflags;

						// Set the flag for the high boundary.
						if (v.discriminate(lop.key) <= 0 && v.discriminate(hip.key) == 1)
							tempflags.second[v.get_discriminator_dim()] = true;

						if (childtype == BLOCK_NODE) {
							if (tempflags.alltrue() && stream == NULL) {
								// No need to recurse, just return the weight of the child node.
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
								result += v.low_weight();
#else
								bn2 = fetch_node(bn->lk[child]);
								result += bn2->weight();
								release_node(bn2);
#endif
							} else {
								s.push(outer_stack_elem(tempflags,
														std::pair<bid_t, link_type_t>(bn->lk[child], childtype)));
							}
						} else if (childtype == BLOCK_LEAF) {
							if (tempflags.alltrue() && stream == NULL) {
								// No need to recurse, just return the weight of the child node.
#if TPIE_AMI_KDREE_STORE_WEIGHTS
								result += v.low_weight();
#else
								bl = fetch_leaf(bn->lk[child]);
								result += bl->weight();
								release_leaf(bl);
#endif
							} else {
								s.push(outer_stack_elem(tempflags,
														std::pair<bid_t, link_type_t>(bn->lk[child], childtype)));
							}
						} else { // BIN_NODE
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
							if (tempflags.alltrue() && stream == NULL)
								result += v.low_weight();
							else
								ss.push(inner_stack_elem(tempflags, child));
#else
							ss.push(inner_stack_elem(tempflags, child));
#endif
						}
					}

					// Check whether we need to visit the high child of v.
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
					if (v.discriminate(lop.key) >= 0 || v.discriminate(hip.key) >= 0) {
#else
						if (v.discriminate(lop.key) == 1 || v.discriminate(hip.key) == 1) {
#endif
							// Push the low child into the appropriate stack.
							v.get_high_child(child, childtype);
							// Again, make a copy of topflags.
							tempflags = topflags;
	  
							// Set the flag for the low boundary.
#if TPIE_AMI_KDTREE_USE_EXACT_SPLIT
							if (v.discriminate(lop.key) < 0 && v.discriminate(hip.key) >= 0)
#else
								if (v.discriminate(lop.key) <= 0 && v.discriminate(hip.key) == 1)
#endif
									tempflags.first[v.get_discriminator_dim()] = true;

							if (childtype == BLOCK_NODE) {
								if (tempflags.alltrue() && stream == NULL) {
									// No need to recurse, just return the weight of the child node.
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
									result += v.high_weight();
#else
									bn2 = fetch_node(bn->lk[child]);
									result += bn2->weight();
									release_node(bn2);
#endif
								} else {
									s.push(outer_stack_elem(tempflags,
															std::pair<bid_t, link_type_t>(bn->lk[child], childtype)));
								}
							} else if (childtype == BLOCK_LEAF) {
								if (tempflags.alltrue() && stream == NULL) {
									// No need to recurse, just return the weight of the child node.
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
									result += v.high_weight();
#else
									bl = fetch_leaf(bn->lk[child]);
									result += bl->weight();
									release_leaf(bl);
#endif
								} else {
									s.push(outer_stack_elem(tempflags,
															std::pair<bid_t, link_type_t>(bn->lk[child], childtype)));
								}
							} else { // BIN_NODE
#if TPIE_AMI_KDTREE_STORE_WEIGHTS
								if (tempflags.alltrue() && stream == NULL)
									result += v.high_weight();
								else
									ss.push(inner_stack_elem(tempflags, child));
#else
								ss.push(inner_stack_elem(tempflags, child));
#endif
							}
						}

					} // while !ss.empty()
      
					// We are done with this block node.
					release_node(bn);
				}

			} // while !s.empty()

			TPLOG("kdtree::window_query Exiting "<<"\n");
			return result;
		}

//// *kdtree::find_leaf* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			bid_t TPIE_AMI_KDTREE::find_leaf(const POINT &p) {
			TPLOG("kdtree::find_leaf Entering "<<"\n");
			std::pair<bid_t, link_type_t> n = 
				std::pair<bid_t, link_type_t>(header_.root_bid, header_.root_type);
			TPIE_AMI_KDTREE_NODE* bn;
			//  bool ans;

			// Go down the tree until the appropriate leaf is found.
			while (n.second == BLOCK_NODE) {
				bn = fetch_node(n.first);
				n = bn->find(p);
				release_node(bn);
			}

			assert(n.second == BLOCK_LEAF);
			TPLOG("kdtree::find_leaf Exiting "<<"\n");
			return n.first;
		}

//// *kdtree::find* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			bool TPIE_AMI_KDTREE::find(const POINT &p) {

			TPLOG("kdtree::find Entering "<<"\n");
			bool ans;

			// Check the leaf.
			TPIE_AMI_KDTREE_LEAF* bl = fetch_leaf(find_leaf(p));
			ans = (bl->find(p) < bl->size());
			release_leaf(bl);

			TPLOG("kdtree::find Exiting ans="<<ans<<"\n"); 
			return ans;
		}

//// *kdtree::insert* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			bool TPIE_AMI_KDTREE::insert(const POINT& p) {
			TPLOG("kdtree::insert Entering "<<"\n");
			bool ans = false;
			TPIE_AMI_KDTREE_LEAF* bl = fetch_leaf(find_leaf(p));

			if (bl->size() == params_.leaf_size_max)
				ans =  false;
			else if ( (ans = bl->insert(p)) )
				header_.size++;

			// TODO: update the weights of all nodes on the path!

			TPLOG("kdtree::insert Exiting "<<"\n");
			return ans;
		}

//// *kdtree::erase* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			bool TPIE_AMI_KDTREE::erase(const POINT& p) {
			TPLOG("kdtree::erase Entering "<<"\n");
			bool ans;

			TPIE_AMI_KDTREE_LEAF* bl = fetch_leaf(find_leaf(p));
			if ((ans = bl->erase(p)))
				header_.size--;
			release_leaf(bl);

			TPLOG("kdtree::erase Exiting "<<"\n");
			return ans;
		}

//// *kdtree::persist* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			void TPIE_AMI_KDTREE::persist(persistence per) {
			TPLOG("kdtree::persist Entering "<<"\n");

			pcoll_leaves_->persist(per);
			pcoll_nodes_->persist(per);

			TPLOG("kdtree::persist Exiting "<<"\n");
		}

//// *kdtree::fetch_node* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			TPIE_AMI_KDTREE_NODE* TPIE_AMI_KDTREE::fetch_node(bid_t bid) {
			TPIE_AMI_KDTREE_NODE* q;
			stats_.record(NODE_FETCH);
			// Warning: using short-circuit evaluation. Order is important.
			if ((bid == 0) || !node_cache_->read(bid, q)) {
				q = new TPIE_AMI_KDTREE_NODE(pcoll_nodes_, bid);
			}
			return q;
		}

//// *kdtree::fetch_leaf* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			TPIE_AMI_KDTREE_LEAF* TPIE_AMI_KDTREE::fetch_leaf(bid_t bid) {
			TPIE_AMI_KDTREE_LEAF* q;
			stats_.record(LEAF_FETCH);
			// Warning: using short-circuit evaluation. Order is important.
			if ((bid == 0) || !leaf_cache_->read(bid, q)) {
				q = new TPIE_AMI_KDTREE_LEAF(pcoll_leaves_, bid);
			}
			return q;
		}

//// *kdtree::release_node* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			void TPIE_AMI_KDTREE::release_node(TPIE_AMI_KDTREE_NODE* q) {
			stats_.record(NODE_RELEASE);
			if (q->persist() == PERSIST_DELETE)
				delete q;
			else
				node_cache_->write(q->bid(), q);
		}

//// *kdtree::release_leaf* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			void TPIE_AMI_KDTREE::release_leaf(TPIE_AMI_KDTREE_LEAF* q) {
			stats_.record(LEAF_RELEASE);
			if (q->persist() == PERSIST_DELETE)
				delete q;
			else
				leaf_cache_->write(q->bid(), q);
		}

//// *kdtree::stats* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			const stats_tree &TPIE_AMI_KDTREE::stats() {
			node_cache_->flush();
			leaf_cache_->flush();
			stats_.set(LEAF_READ, pcoll_leaves_->stats().get(BLOCK_GET));
			stats_.set(LEAF_WRITE, pcoll_leaves_->stats().get(BLOCK_PUT));
			stats_.set(LEAF_CREATE, pcoll_leaves_->stats().get(BLOCK_NEW));
			stats_.set(LEAF_DELETE, pcoll_leaves_->stats().get(BLOCK_DELETE));
			stats_.set(LEAF_COUNT, pcoll_leaves_->size());
			stats_.set(NODE_READ, pcoll_nodes_->stats().get(BLOCK_GET));
			stats_.set(NODE_WRITE, pcoll_nodes_->stats().get(BLOCK_PUT));
			stats_.set(NODE_CREATE, pcoll_nodes_->stats().get(BLOCK_NEW));
			stats_.set(NODE_DELETE, pcoll_nodes_->stats().get(BLOCK_DELETE));
			stats_.set(NODE_COUNT, pcoll_nodes_->size());
			return stats_;
		}

//// *kdtree::print* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			void TPIE_AMI_KDTREE::print(std::ostream& s) {
			s << "kdtree nodes: ";
			if (header_.root_type == BLOCK_NODE) {
				TPIE_AMI_KDTREE_NODE* bn;
				std::queue<bid_t> xq; //  external queue; stores node id's
				std::queue<size_t> iq; // internal queue;
				size_t i, idx, fo;
				link_type_t idx_type;

				xq.push(header_.root_bid);
				//    level = 0;
				while (!xq.empty()) {
					bn = fetch_node(xq.front());
					xq.pop();
					assert(iq.empty());
					iq.push(0);
					fo = 0;

					s << "[id=" << bn->bid() << " (";

					while (!iq.empty()) {
						i = iq.front();
						iq.pop();

						s << "B" << (TPIE_OS_OFFSET)bn->el[i].get_discriminator_dim() << " " 
						  << bn->el[i].get_discriminator_val() << "\n";

						bn->el[i].get_low_child(idx, idx_type);

						if (idx_type == BIN_NODE) {
							iq.push(idx);
						} else {
							fo++;
							if (idx_type == BLOCK_NODE) {
								s << "N" << bn->lk[idx];
								xq.push(bn->lk[idx]);
							} else {
								s << "L";
							}
							s << " ";
						}


						bn->el[i].get_high_child(idx, idx_type);

						if (idx_type == BIN_NODE) {
							iq.push(idx);
						} else {
							fo++;
							if (idx_type == BLOCK_NODE) {
								s << "N" << bn->lk[idx];
								xq.push(bn->lk[idx]);
							} else {
								s << "L";
							}
							s << " ";
						}
					}
					s << "\b) fo=" << (TPIE_OS_OFFSET)fo << "]\n";
					release_node(bn);
				}
			} else
				s << " Root is leaf.\n";
			s << "\n";
		}


//// *kdtree::print* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			void TPIE_AMI_KDTREE::print(std::ostream& s, bool print_mbr, bool print_level, char indent_char) {


			//  s << "kdtree nodes: ";
			if (header_.root_type == BLOCK_NODE) {

				TPIE_AMI_KDTREE_NODE* bln;
				TPIE_AMI_KDTREE_LEAF* bll;
				// The current binary node.
				Bin_node bin;

				// The recursion stack.
				std::stack<print_stack_elem> dfs_stack;
				
				point_t rlo, rhi;
				size_t i, j, idx;
				link_type_t idx_type;

				// Initialize the stack.
				dfs_stack.push(print_stack_elem(header_.root_bid, 0, 0, header_.mbr_lo, header_.mbr_hi));

				while (!dfs_stack.empty()) {

					rlo = dfs_stack.top().lo;
					rhi = dfs_stack.top().hi;

					if (dfs_stack.top().idx == -1) { // Top of the stack is a leaf.
						// Print the leaf.
	
						// The MBR.
						if (print_mbr) {
							s << "[";
							s << "(";
							for (j = 0; j < dim-1; j++) {
								s << rlo[j] << ",";
							}
							s << rlo[dim-1] << ") ";
							s << "(";
							for (j = 0; j < dim-1; j++) {
								s << rhi[j] << ",";
							}
							s << rhi[dim-1] << ")";
							s << "] ";
						}
						if (print_level) {
							s << (dfs_stack.size()-1) << (dfs_stack.size()-1 < 10 ? "  ": " ");
						}
						for (i = 0; i < dfs_stack.size()-1; i++) {
							s << indent_char;
						}
						s << "L ";
						bll = fetch_leaf(dfs_stack.top().bid);
						for (i = 0; i < bll->size(); i++) {
							s << "(";
							for (j = 0; j < dim-1; j++) {
								s << bll->el[i][j] << ",";
							}
							s << bll->el[i][dim-1] << ") ";
						}
						s << std::endl;
						release_leaf(bll);

						dfs_stack.pop();	  

					} else { // Top of the stack is a node.

						bln = fetch_node(dfs_stack.top().bid);
						bin = bln->el[dfs_stack.top().idx];

						if (dfs_stack.top().visits == 0) {

							// Print the binary node 'bin', since it's the first time we see it.

							// The MBR.
							if (print_mbr) {
								s << "[";
								s << "(";
								for (j = 0; j < dim-1; j++) {
									s << rlo[j] << ",";
								}
								s << rlo[dim-1] << ") ";
								s << "(";
								for (j = 0; j < dim-1; j++) {
									s << rhi[j] << ",";
								}
								s << rhi[dim-1] << ")";
								s << "] ";
							}
							if (print_level) {
								s << (dfs_stack.size()-1) << (dfs_stack.size()-1 < 10 ? "  ": " ");
							}
							for (i = 0; i < dfs_stack.size()-1; i++) {
								s << indent_char;
							}

							s << "B" << bin.get_discriminator_dim();
							s << " " << bin.get_discriminator_val();
							s << std::endl;
	  
							bin.get_low_child(idx, idx_type);
							rhi[bin.get_discriminator_dim()] = bin.get_discriminator_val();

						} else {
							bin.get_high_child(idx, idx_type);
							rlo[bin.get_discriminator_dim()] = bin.get_discriminator_val();
						}
	
						dfs_stack.top().visits++;
	
						if (idx_type == BIN_NODE) {
							dfs_stack.push(print_stack_elem(bln->bid(), idx, 0, rlo, rhi));
						} else if (idx_type == BLOCK_NODE) {
							dfs_stack.push(print_stack_elem(bln->lk[idx], 0, 0, rlo, rhi));
						} else { // idx_type == BLOCK_LEAF
							dfs_stack.push(print_stack_elem(bln->lk[idx], -1, 0, rlo, rhi));
						}
	
						release_node(bln);
	
					}

					while (!dfs_stack.empty() && dfs_stack.top().visits == 2) {
						dfs_stack.pop();
					}

				}

			} else {
				s << "Root is leaf." << std::endl;
			}

			s << std::endl;
		}



//// *kdtree::~kdtree* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			TPIE_AMI_KDTREE::~kdtree() {
			TPLOG("kdtree::~kdtree Entering status="<<status_<<"\n");

			if (status_ == KDTREE_STATUS_VALID) {
				// Write initialization info into the pcoll_nodes_ header.
				//    *((header_t *) pcoll_nodes_->user_data()) = header_;
				memcpy(pcoll_nodes_->user_data(), (void *)(&header_), sizeof(header_));
			}
			// Delete the comparison objects.
			for (size_t i = 0; i < dim; i++) {
				delete comp_obj_[i];
			}

			delete node_cache_;
			delete leaf_cache_;

			// Delete the two collections.
			delete pcoll_nodes_;
			delete pcoll_leaves_;

			TPLOG("kdtree::~kdtree Exiting status="<<status_<<"\n");
		}

//// *kdtree::create_sample* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			void TPIE_AMI_KDTREE::create_sample(bid_t&, TPIE_OS_SIZE_T, POINT_STREAM* in_stream) {

			// New sample.
			DBG("  Sampling [" << 20000 << " (random seek + read)]...\n");
			gso = new sample(20000, in_stream);

			DBG("  Creating top levels [very few node writes]...\n");
			// Dirty tricks. TODO: cleanup.
			size_t save_leaf_size_max = params_.leaf_size_max;
			points_are_sample  = true;
			params_.leaf_size_max = 5000;
			while (!can_do_mm(size_t(((1.1 * in_stream->stream_len()) / gso->sz) * 
									 params_.leaf_size_max)))
				params_.leaf_size_max -= 50;
			if (params_.leaf_size_max == 0)
				params_.leaf_size_max = 40;

			create_node_mm(header_.root_bid, 0, gso->mm_streams, gso->sz);
			params_.leaf_size_max = save_leaf_size_max;
			points_are_sample = false;
			gso->cleanup();

			DBG("  Distributing into " << (TPIE_OS_OFFSET)gso->q.size() << " streams...\n");
			distribute_s(header_.root_bid, 0, gso);

			DBG("  Building lower levels...\n");
			build_lower_tree_s(gso);

			delete gso;
		}

//// *kdtree::build_lower_tree_s* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			void TPIE_AMI_KDTREE::build_lower_tree_s(sample* s) {
			sample_context* sc;
			size_t j;
			//  POINT* p;
			TPIE_AMI_KDTREE_NODE* b;
			TPIE_OS_SIZE_T next_free_el, next_free_lk;
			TPIE_OS_SIZE_T sz;
			POINT* streams_mm[dim];

			for (j = 0; j < s->q.size(); j++) {

				sc = &s->q[j];

				b = fetch_node(sc->bid);
				next_free_el = b->size();
				next_free_lk = (TPIE_OS_SIZE_T)b->lk[b->lk.capacity()-1];
				b->lk[b->lk.capacity()-1] = 0;

				DBG("L");
				sc->stream = new POINT_STREAM(sc->stream_name);
				sc->stream->persist(PERSIST_DELETE);
				if (!sc->stream->is_valid()) {
					std::cerr << "kdtree bulk loading internal error.\n"
							  << "[invalid stream restored from file]\n";
					std::cerr << "Skipping.\n";
					delete sc->stream;
					sc->stream = NULL;
					continue;
				}

				if (!can_do_mm(sc->stream->stream_len())) {
					std::cerr << "Temp stream too big: " 
							  << sc->stream->stream_len() << " items.\n";
					std::cerr << "Aborting.\n";
					exit(1);
				}
				copy_to_mm(sc->stream, streams_mm, sz);
				delete sc->stream;
				sc->stream = NULL;

				DBG("B" << (TPIE_OS_OFFSET)sz);
				// Build the subtree.

				if (sz <= params_.leaf_size_max) {

					if (sc->low)
						b->el[sc->ctx.i].set_low_child(next_free_lk++, BLOCK_LEAF);
					else
						b->el[sc->ctx.i].set_high_child(next_free_lk++, BLOCK_LEAF);
					TPLOG("  b("<<b->bid()<<")->el["<<sc->ctx.i<<"].?: ("<<next_free_lk-1<<", BLOCK_LEAF)\n");
					create_leaf_mm(b->lk[next_free_lk - 1], (sc->ctx.d + 1) % dim, streams_mm, sz);

				} else if ((sc->ctx.h + 1 >= max_intranode_height(b->bid())) || 
						   (next_free_el >= params_.node_size_max)) {

					if (sc->low)
						b->el[sc->ctx.i].set_low_child(next_free_lk++, BLOCK_NODE);
					else
						b->el[sc->ctx.i].set_high_child(next_free_lk++, BLOCK_NODE);
					TPLOG("  b("<<b->bid()<<")->el["<<sc->ctx.i<<"].?: ("<<next_free_lk-1<<", BLOCK_NODE)\n");
					create_node_mm(b->lk[next_free_lk - 1], (sc->ctx.d + 1) % dim, streams_mm, sz);

				} else {

					if (sc->low)
						b->el[sc->ctx.i].set_low_child(next_free_el++, BIN_NODE);
					else
						b->el[sc->ctx.i].set_high_child(next_free_el++, BIN_NODE);
					TPLOG("  b("<<b->bid()<<")->el["<<sc->ctx.i<<"].?: ("<<next_free_el-1<<", BIN_NODE)\n");
					///DBG("create_grid: sz=" << sz << "\n"); 
					create_bin_node_mm(b, bn_context(next_free_el - 1, sc->ctx.h + 1, 
													 (sc->ctx.d + 1) % dim),
									   streams_mm, sz, next_free_el, next_free_lk);
				}

				// save the next_free_* info.
				if (b->lk[b->lk.capacity()-1] == 0) {
					b->size() = next_free_el;
					b->lk[b->lk.capacity()-1] = next_free_lk;
				}
    
				release_node(b);

				DBG(" ");
			}
		}

//// *kdtree::distribute_s* ////
		template<class coord_t, TPIE_OS_SIZE_T dim, class Bin_node, class BTECOLL>
			void TPIE_AMI_KDTREE::distribute_s(bid_t bid, TPIE_OS_SIZE_T, sample* s) {

			TPIE_AMI_KDTREE_NODE* n, *r = fetch_node(bid);
			bid_t nbid;
			POINT* p;
			std::pair<size_t, link_type_t> a;
			TPIE_OS_SIZE_T j;
			TPIE_OS_OFFSET sz;

			// Create all streams. Could we run out of memory here?
			for (j = 0; j < s->q.size(); j++) {
				s->q[j].stream = new POINT_STREAM;
				s->q[j].stream_name = s->q[j].stream->name();
				s->q[j].stream->persist(PERSIST_PERSISTENT);
			}

			s->in_stream->seek(0);
			j = 0;
			sz = s->in_stream->stream_len();
			while ((s->in_stream->read_item(&p)) == tpie::ami::NO_ERROR) {
				if (j % 200000 == 0)
					DBG("\b\b\b"<<TPIE_OS_OFFSET(((float)j / sz) * 100.0)<<"%");
				j++;
				a = r->find_index(*p);
				if (a.second == BLOCK_NODE)
					nbid = r->lk[a.first];
				while (a.second == BLOCK_NODE) {
					n = fetch_node(nbid);
					a = n->find_index(*p);
					if (a.second == BLOCK_NODE)
						nbid = n->lk[a.first];
					release_node(n);
				}

				assert(a.second == GRID_INDEX);
				assert(a.first < s->q.size());

				s->q[a.first].stream->write_item(*p);
			}

			DBG("\b\b\b   \b\b\b");
			release_node(r);

			// Delete output streams. We'll read them later, one by one.
			for (j = 0; j < s->q.size(); j++) {
				delete s->q[j].stream;
				s->q[j].stream = NULL;
			}
		}



#undef TPIE_AMI_KDTREE_LEAF
#undef TPIE_AMI_KDTREE_NODE
#undef TPIE_AMI_KDTREE     
#undef POINT       
#undef POINT_STREAM

	} } //end of tpie::ami namespace

#endif //_TPIE_AMI_KDTREE_H
