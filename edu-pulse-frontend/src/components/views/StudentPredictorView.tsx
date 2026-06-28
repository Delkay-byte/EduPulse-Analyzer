import React from 'react';
import { Brain, TrendingUp, AlertTriangle } from 'lucide-react';

export const StudentPredictorView = () => {
  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      <div>
        <h2 className="text-2xl font-black text-white">Student Performance Predictor</h2>
        <p className="text-slate-400">AI-powered predictions based on historical performance data.</p>
      </div>

      <div className="bg-slate-900 p-8 rounded-2xl border border-slate-800">
        <div className="flex items-center gap-2 text-purple-500 mb-6">
          <Brain size={20} />
          <h3 className="font-bold uppercase tracking-wider text-sm">Prediction Model</h3>
        </div>
        
        <div className="text-center py-12">
          <TrendingUp size={48} className="text-slate-600 mx-auto mb-4" />
          <p className="text-slate-400">Predictive analytics module coming soon.</p>
          <p className="text-slate-500 text-sm mt-2">This feature will use machine learning to predict student outcomes.</p>
        </div>
      </div>

      <div className="bg-slate-900 p-6 rounded-2xl border border-slate-800">
        <div className="flex items-center gap-2 text-amber-500 mb-4">
          <AlertTriangle size={20} />
          <h3 className="font-bold uppercase tracking-wider text-sm">At-Risk Students</h3>
        </div>
        <p className="text-slate-400 text-sm">Students identified as high-risk based on current performance trends will appear here.</p>
      </div>
    </div>
  );
};
